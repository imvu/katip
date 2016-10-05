{-# LANGUAGE BangPatterns  #-}
{-# LANGUAGE DeriveDataTypeable  #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
-- | Includes a scribe that can be used to log structured, JSON log
-- messages to ElasticSearch. These logs can be explored easily using
-- <https://www.elastic.co/products/kibana kibana> or your tool of
-- choice.
--
-- == __Important Note on Index Settings__
--
-- 'defaultEsScribeCfg' inherits a set of default index settings from
-- the @bloodhound@ package. These settings at this time of writing
-- set the indices up to have 3 shards and 2 replicas. This is an
-- arguably reasonable default setting for production but may cause
-- problems for development. In development, your cluster may be
-- configured to seek a write quorum greater than 1. If you're running
-- ElasticSearch on a single node, this could cause your writes to
-- wait for a bit and then fail due to a lack of quorum. __For development, we recommend setting your replica count to 0 or modifying your write quorum settings__. For production, we recommend reading the
-- <https://www.elastic.co/guide/en/elasticsearch/guide/current/scale.html ElasticSearch Scaling Guide> and choosing the appropriate settings,
-- keeping in mind that you can chage replica counts on a live index
-- but that changing shard counts requires recreating the index.
module Katip.Scribes.ElasticSearch
    (-- * Building a scribe
      mkEsScribe
    -- * Scribe configuration
    , EsScribeSetupError(..)
    , EsQueueSize
    , mkEsQueueSize
    , EsPoolSize
    , mkEsPoolSize
    , EsScribeCfg
    , essRetryPolicy
    , essQueueSize
    , essPoolSize
    , essAnnotateTypes
    , essIndexSettings
    , essIndexSharding
    , IndexShardingPolicy(..)
    , IndexNameSegment(..)
    , essQueueSendThreshold
    , QueueSendThreshold(..)
    , BulkSendCfg(..)
    , BulkSendType(..)
    , Timeout(..)
    , Seconds(..)
    , MicroSeconds(..)
    , essLoggingGuarantees
    , LoggingGuarantees(..)
    , essShouldAdminES
    , ShouldAdminES(..)
    , essDebugCallback
    , DebugCallback(..)
    , DebugStatus(..)
    , ReportFn
    , ReportSignal
    , ShouldSignalBlock(..)
    , defaultEsScribeCfg
    -- * Utilities
    , mkDocId
    , module Katip.Scribes.ElasticSearch.Annotations
    , roundToSunday
    ) where

-------------------------------------------------------------------------------
import           Control.Applicative                     as A
import           Control.Concurrent
import           Control.Concurrent.Async
import           Control.Concurrent.STM.TBMQueue
import           Control.Concurrent.STM.TVar
import           Control.Concurrent.STM.TBChan
import           Control.Exception.Base
import           Control.Exception.Enclosed
import           Control.Monad
import           Control.Monad.Catch
import           Control.Monad.IO.Class
import           Control.Monad.STM
import           Control.Retry                           (RetryPolicy,
                                                          exponentialBackoff,
                                                          limitRetries,
                                                          constantDelay,
                                                          retrying,
                                                          recovering)
import           Data.Aeson
import           Data.Monoid                             ((<>))
import           Data.Text                               (Text)
import qualified Data.Text                               as T
import qualified Data.Text.Encoding                      as T
import           Data.Time
import           Data.Time.Calendar.WeekDate
import           Data.Time.Clock.POSIX
import           Data.Typeable
import           Data.UUID
import qualified Data.UUID.V4                            as UUID4
import qualified Data.Vector                             as V
import           Database.Bloodhound                     hiding (Seconds)
import           Network.HTTP.Client
import           Network.HTTP.Types.Status
import           Text.Printf                             (printf)
-------------------------------------------------------------------------------
import           Katip.Core
import           Katip.Scribes.ElasticSearch.Annotations
-------------------------------------------------------------------------------

data EsScribeCfg = EsScribeCfg {
      essRetryPolicy          :: !RetryPolicy
    -- ^ Retry policy when there are errors sending logs to the server
    , essQueueSize            :: !EsQueueSize
    -- ^ Maximum size of the bounded log queue
    , essPoolSize             :: !EsPoolSize
    -- ^ Worker pool size limit for sending data to the
    , essAnnotateTypes        :: !Bool
    -- ^ Different payload items coexist in the "data" attribute in
    -- ES. It is possible for different payloads to have different
    -- types for the same key, e.g. an "id" key that is sometimes a
    -- number and sometimes a string. If you're having ES do dynamic
    -- mapping, the first log item will set the type and any that
    -- don't conform will be *discarded*. If you set this to True,
    -- keys will recursively be appended with their ES core
    -- type. e.g. "id" would become "id::l" and "id::s"
    -- automatically, so they won't conflict. When this library
    -- exposes a querying API, we will try to make deserialization and
    -- querying transparently remove the type annotations if this is
    -- enabled.
    , essIndexSettings        :: !IndexSettings
    , essIndexSharding        :: !IndexShardingPolicy
    , essQueueSendThreshold   :: !QueueSendThreshold
    -- ^ Configures how logs should be batched
    , essLoggingGuarantees    :: !LoggingGuarantees
    -- ^ What kind of guarantee is made that a log message will be queued
    , essShouldAdminES        :: !ShouldAdminES
    -- ^ Whether or not the application should manage es (createIndex / update index) or if this is managed by another team
    , essDebugCallback        :: !DebugCallback
    -- ^ Since logging relies on this being configured, give an escape hatch for debugging why logging fails
    } deriving (Typeable)

-- | Reasonable defaults for a config:
--
--     * defaultManagerSettings
--
--     * exponential backoff with 25ms base delay up to 5 retries
--
--     * Queue size of 1000
--
--     * Pool size of 2
--
--     * Annotate types set to False
--
--     * DailyIndexSharding
--
--     * SendEach message eagerly
--
--     * NoGuarantee on writing log messages
--
--     * This application can administer elasticsearch
--
--     * No callback
defaultEsScribeCfg :: EsScribeCfg
defaultEsScribeCfg = EsScribeCfg {
      essRetryPolicy          = exponentialBackoff 25 <> limitRetries 5
    , essQueueSize            = EsQueueSize 1000
    , essPoolSize             = EsPoolSize 2
    , essAnnotateTypes        = False
    , essIndexSettings        = defaultIndexSettings
    , essIndexSharding        = DailyIndexSharding
    , essQueueSendThreshold   = SendEach
    , essLoggingGuarantees    = NoGuarantee
    , essShouldAdminES        = AdminES
    , essDebugCallback        = NoCallback
    }

-------------------------------------------------------------------------------
-- | How should katip store your log data?
--
-- * NoIndexSharding will store all logs in one index name. This is
-- the simplest option but is not advised in production. In practice,
-- the index will grow very large and will get slower to
-- search. Deleting records based on some sort of retention period is
-- also extremely slow.
--
-- * MonthlyIndexSharding, DailyIndexSharding, HourlyIndexSharding,
-- EveryMinuteIndexSharding will generate indexes based on the time of
-- the log. Index name is treated as a prefix. So if your index name
-- is @foo@ and DailySharding is used, logs will be stored in
-- @foo-2016-2-25@, @foo-2016-2-26@ and so on. Index templating will
-- be used to set up mappings automatically. Deletes based on date are
-- very fast and queries can be restricted to date ranges for better
-- performance. Queries against all dates should use @foo-*@ as an
-- index name. Note that index aliasing's glob feature is not suitable
-- for these date ranges as it matches index names as they are
-- declared, so new dates will be excluded. DailyIndexSharding is a
-- reasonable choice. Changing index sharding strategies is not
-- advisable.
--
-- * CustomSharding: supply your own function that decomposes an item
-- into its index name heirarchy which will be appended to the index
-- name. So for instance if your function return ["arbitrary",
-- "prefix"], the index will be @foo-arbitrary-prefix@ and the index
-- template will be set to match @foo-*@. In general, you want to use
-- segments of increasing granularity (like year, month, day for
-- dates). This makes it easier to address groups of indexes
-- (e.g. @foo-2016-*@).
data IndexShardingPolicy = NoIndexSharding
                         | MonthlyIndexSharding
                         | WeeklyIndexSharding
                         -- ^ A special case of daily which shards to sunday
                         | DailyIndexSharding
                         | HourlyIndexSharding
                         | EveryMinuteIndexSharding
                         | CustomIndexSharding (forall a. Item a -> [IndexNameSegment])

instance Show IndexShardingPolicy where
  show NoIndexSharding          = "NoIndexSharding"
  show MonthlyIndexSharding     = "MonthlyIndexSharding"
  show WeeklyIndexSharding      = "WeeklyIndexSharding"
  show DailyIndexSharding       = "DailyIndexSharding"
  show HourlyIndexSharding      = "HourlyIndexSharding"
  show EveryMinuteIndexSharding = "EveryMinuteIndexSharding"
  show (CustomIndexSharding _)  = "CustomIndexSharding λ"

-------------------------------------------------------------------------------
newtype IndexNameSegment = IndexNameSegment {
      indexNameSegment :: Text
    } deriving (Show, Eq, Ord)

-------------------------------------------------------------------------------
shardPolicySegs :: IndexShardingPolicy -> Item a -> [IndexNameSegment]
shardPolicySegs NoIndexSharding _ = []
shardPolicySegs MonthlyIndexSharding Item {..} = [sis y, sis m]
  where
    (y, m, _) = toGregorian (utctDay _itemTime)
shardPolicySegs WeeklyIndexSharding Item {..} = [sis y, sis m, sis d]
  where
    (y, m, d) = toGregorian (roundToSunday (utctDay _itemTime))
shardPolicySegs DailyIndexSharding Item {..} = [sis y, sis m, sis d]
  where
    (y, m, d) = toGregorian (utctDay _itemTime)
shardPolicySegs HourlyIndexSharding Item {..} = [sis y, sis m, sis d, sis h]
  where
    (y, m, d) = toGregorian (utctDay _itemTime)
    (h, _) = splitTime (utctDayTime _itemTime)
shardPolicySegs EveryMinuteIndexSharding Item {..} = [sis y, sis m, sis d, sis h, sis mn]
  where
    (y, m, d) = toGregorian (utctDay _itemTime)
    (h, mn) = splitTime (utctDayTime _itemTime)
shardPolicySegs (CustomIndexSharding f) i  = f i

-------------------------------------------------------------------------------
-- | If the given day is sunday, returns the input, otherwise returns
-- the previous sunday
roundToSunday :: Day -> Day
roundToSunday d
    | dow == 7  = d
    | w > 1     = fromWeekDate y (w - 1) 7
    | otherwise = fromWeekDate (y - 1) 53 7
  where
    (y, w, dow) = toWeekDate d

-------------------------------------------------------------------------------
chooseIxn :: IndexName -> IndexShardingPolicy -> Item a -> IndexName
chooseIxn (IndexName ixn) p i =
  IndexName (T.intercalate "-" (ixn:segs))
  where
    segs = indexNameSegment A.<$> shardPolicySegs p i

-------------------------------------------------------------------------------
sis :: Integral a => a -> IndexNameSegment
sis = IndexNameSegment . T.pack . fmt
  where
    fmt = printf "%02d" . toInteger

-------------------------------------------------------------------------------
splitTime :: DiffTime -> (Int, Int)
splitTime t = asMins `divMod` 60
  where
    asMins = floor t `div` 60

-------------------------------------------------------------------------------
newtype Seconds = Seconds { unSeconds :: Int }
                deriving (Typeable)

-------------------------------------------------------------------------------
newtype MicroSeconds = MicroSeconds { unMicroSeconds :: Int }
                     deriving (Typeable)

-------------------------------------------------------------------------------
-- | Configurable timeout for bulk sending
data Timeout = Timeout Seconds MicroSeconds
             -- ^ Configure to timeout using a set time
             | TimeoutExt (TVar Bool)
             -- ^ Configure to timeout using a synchronized variable
             deriving (Typeable)

-------------------------------------------------------------------------------
data QueueSendThreshold = SendEach
                        -- ^ Log each element using indexDocument
                        | BulkSend !BulkSendCfg
                        -- ^ Use a bulk strategy when logging the document
                        deriving (Typeable)

-------------------------------------------------------------------------------
data BulkSendCfg = BulkSendCfg {
    bscTimeout              :: !Timeout
  , bscBulkSendType         :: !BulkSendType
  , bscAccumulationDelay    :: !MicroSeconds
  }
  deriving (Typeable)

-------------------------------------------------------------------------------
data BulkSendType = SendThresholdCount !Int
                  -- ^ Simple count threshold for bulk sending. Try to get up to the number of log elements to send
                  | SendThresholdPredicate ([(IndexName,Value)] -> Bool)
                  -- ^ User provided predicate for how to determine when to stop accumulating log elements for bulk sending
                  deriving (Typeable)

-------------------------------------------------------------------------------
data LoggingGuarantees = NoGuarantee
                       -- ^ Try to write the message to the queue once, if it fails, it fails
                       | Try !Int
                       -- ^ Try up to N times to send the message to the queue (with a small delay between messages)
                       | TryWithPolicy !RetryPolicy
                       -- ^ Try using a retry policy
                       | TryAll
                       -- ^ Keep trying every millisecond to queue the message.
                       deriving (Typeable)

-------------------------------------------------------------------------------
-- | Configures the scribe with some level of elastic search index configuration
--
-- In certain environments the software should not be allowed to administer ES at all
data ShouldAdminES = NoAdminES
                   -- ^ Do no ES administartion. Its up to someone else
                   | CheckExistsNoAdminES
                   -- ^ Check that the index exists
                   | AdminES
                   -- ^ Fully administer the index (create and check)
                   deriving (Typeable)

-------------------------------------------------------------------------------
-- | Debug callback to report / signal information about the scribes state
--
-- Useful for testing and reporting why something may not be working / started in production
data DebugCallback = DebugCallback { unReportFn :: ReportFn
                                   -- ^ Logging for the scribes setup
                                   , unReportSignal :: ReportSignal
                                   -- ^ Signal that can be used to get reports about the scribe
                                   , unReportSchedule :: !Int
                                   -- ^ How often to report queue information
                                   }
                   -- ^ Configure a callback
                   | NoCallback
                   -- ^ Configure no callback
                   deriving (Typeable)

-- | Alias for a reporting function
type ReportFn = Maybe (T.Text -> IO ())

-- | Should signals block
data ShouldSignalBlock = SignalBlock
                       | SignalNoBlock
                       deriving (Eq, Typeable)

-- | Alias for a reporting signal
type ReportSignal = Maybe (ShouldSignalBlock, TBChan DebugStatus)

-- | Reporting signal messages
data DebugStatus = DSSent !Int
                 -- ^ How many messages were sent
                 | DSSendTimeTaken !Double
                 -- ^ How long did sending the bulk take
                 | DSStartWait
                 -- ^ Waiting on timeout signal
                 | DSFinishWait
                 -- ^ Done waiting on timeout signal
                 | DSTimeTaken !Double
                 -- ^ How long did we wait
                 | DSEstimateLength !Int
                 -- ^ Estimated length of this queue
                 | DSTrueLength !Int
                 -- ^ True length of this queue
                 deriving (Eq, Typeable)


-------------------------------------------------------------------------------
data EsScribeSetupError = CouldNotCreateIndex !Reply
                        | CouldNotCreateMapping !Reply
                        | IndexDoesNotExist deriving (Typeable, Show)

instance Exception EsScribeSetupError


-------------------------------------------------------------------------------
checkIndexExists
    :: ShouldAdminES
    -> Bool
checkIndexExists AdminES = True
checkIndexExists CheckExistsNoAdminES = True
checkIndexExists NoAdminES = False

-------------------------------------------------------------------------------
shouldAdminES
    :: ShouldAdminES
    -> Bool
shouldAdminES AdminES = True
shouldAdminES CheckExistsNoAdminES = False
shouldAdminES NoAdminES = False

-------------------------------------------------------------------------------
report
    :: DebugCallback
    -> T.Text
    -> IO ()
report (DebugCallback (Just f) _ _) t = f t
report _ _ = pure ()

-------------------------------------------------------------------------------
signal
    :: DebugCallback
    -> DebugStatus
    -> IO ()
signal (DebugCallback _ (Just s) _) i = atomically $ signalWrite s i
signal _ _ = pure ()

signalWrite
    :: (ShouldSignalBlock, TBChan DebugStatus)
    -> DebugStatus
    -> STM ()
signalWrite (sb, s) i = case sb of
  SignalBlock -> writeTBChan s i
  SignalNoBlock -> void $ tryWriteTBChan s i

-------------------------------------------------------------------------------
mkEsScribe
    :: EsScribeCfg
    -> BHEnv
    -> IndexName
    -- ^ Treated as a prefix if index sharding is enabled
    -> MappingName
    -> Severity
    -> Verbosity
    -> IO (Scribe, IO ())
    -- ^ Returns a finalizer that will gracefully flush all remaining logs before shutting down workers
mkEsScribe cfg@EsScribeCfg {..} env ix mapping sev verb = do
  q <- newTBMQueueIO $ unEsQueueSize essQueueSize
  endSig <- newEmptyMVar

  when (checkIndexExists essShouldAdminES) $
    runBH env $ do
      report' $ "Checking if index " <> (T.pack $ show ix) <> " exists"
      chk <- indexExists ix
      -- note that this doesn't update settings. That's not available
      -- through the Bloodhound API yet
      if chk
        then pure ()
        else if shouldAdminES essShouldAdminES
          then void $ do
            report' $ "Creating the index " <> (T.pack $ show ix)
            r1 <- createIndex essIndexSettings ix
            unless (statusIsSuccessful (responseStatus r1)) $
              liftIO $ throwIO (CouldNotCreateIndex r1)
            r2 <- if shardingEnabled
              then putTemplate tpl tplName
              else putMapping ix mapping (baseMapping mapping)
            unless (statusIsSuccessful (responseStatus r2)) $
              liftIO $ throwIO (CouldNotCreateMapping r2)
          else liftIO $ throwIO IndexDoesNotExist

  report' $ "Making workers: count(" <> (T.pack $ show essPoolSize) <> ")"
  workers <- replicateM (unEsPoolSize essPoolSize) $ async $
    case essQueueSendThreshold of
      SendEach -> report' "Starting a single log worker" >> startWorker cfg env mapping q
      BulkSend b -> report' "Starting a bulk log worker" >> startBulkWorker cfg env b mapping q


  eReporter <- startQueueReporting essDebugCallback essQueueSize q

  _ <- async $ do
    takeMVar endSig
    atomically $ closeTBMQueue q
    mapM_ waitCatch workers
    case eReporter of
      Left _ -> return ()
      Right reporter -> do
        cancel reporter
        void $ waitCatch reporter
    putMVar endSig ()

  let scribe = Scribe $ \ i ->
        when (_itemSeverity i >= sev) $
          void $ writeAction q essLoggingGuarantees i
  let finalizer = putMVar endSig () >> takeMVar endSig
  return (scribe, finalizer)
  where
    report' text = liftIO $ report essDebugCallback text
    tplName = TemplateName ixn
    shardingEnabled = case essIndexSharding of
      NoIndexSharding -> False
      _               -> True
    tpl = IndexTemplate (TemplatePattern (ixn <> "-*")) (Just essIndexSettings) [toJSON (baseMapping mapping)]
    IndexName ixn = ix
    itemJson' i
      | essAnnotateTypes = itemJson verb (TypeAnnotated <$> i)
      | otherwise        = itemJson verb i

    writeToQueue q i = atomically $ tryWriteTBMQueue q (chooseIxn ix essIndexSharding i, itemJson' i)

    writeAction q NoGuarantee i = writeToQueue q i
    writeAction q (Try tries) i = retryWrite (constantDelay 1000 <> limitRetries tries) q i
    writeAction q (TryWithPolicy retryPolicy) i = retryWrite retryPolicy q i
    writeAction q TryAll i = retryWrite (constantDelay 1000) q i

    checkFailedWrite queueElem = case queueElem of
        Just False -> True
        _ -> False

    retryWrite retryPolicy q i = retrying retryPolicy (const $ pure . checkFailedWrite) $ \_ -> writeToQueue q i

startQueueReporting
    :: DebugCallback
    -> EsQueueSize
    -> TBMQueue (IndexName, Value)
    -> IO (Either () (Async ()))
startQueueReporting (DebugCallback _ (Just t) delay) (EsQueueSize size) q =
  let act = async $ forever $ do
        let estimate = do
              threadDelay delay
              atomically $ do
                estimateLen <- estimateFreeSlotsTBMQueue q
                signalWrite t (DSEstimateLength $ size - estimateLen)
            true = do
              threadDelay delay
              atomically $ do
                trueLen <- freeSlotsTBMQueue q
                signalWrite t (DSTrueLength $ size - trueLen)
        replicateM_ 4 estimate
        true
  in Right <$> act
startQueueReporting _ _ _ = return $ Left ()

-------------------------------------------------------------------------------
baseMapping :: MappingName -> Value
baseMapping (MappingName mn) =
  object [ mn .= object ["properties" .= object prs] ]
  where prs = [ str "thread"
              , str "sev"
              , str "pid"
              , str "ns"
              , str "msg"
              , "loc" .= locType
              , str "host"
              , str "env"
              , "at" .= dateType
              , str "app"
              ]
        str k = k .= object ["type" .= String "string"]
        locType = object ["properties" .= object locPairs]
        locPairs = [ str "loc_pkg"
                   , str "loc_mod"
                   , str "loc_ln"
                   , str "loc_fn"
                   , str "loc_col"
                   ]
        dateType = object [ "format" .= esDateFormat
                          , "type" .= String "date"
                          ]

-------------------------------------------------------------------------------
-- | Handle both old-style aeson and picosecond-level precision
esDateFormat :: Text
esDateFormat = "yyyy-MM-dd'T'HH:mm:ssZ||yyyy-MM-dd'T'HH:mm:ss.SSSZ||yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSSSSZ"

-------------------------------------------------------------------------------
mkDocId :: IO DocId
mkDocId = (DocId . T.decodeUtf8 . toASCIIBytes) `fmap` UUID4.nextRandom

-------------------------------------------------------------------------------
newtype EsQueueSize = EsQueueSize {
       unEsQueueSize :: Int
     } deriving (Show, Eq, Ord)

instance Bounded EsQueueSize where
  minBound = EsQueueSize 1
  maxBound = EsQueueSize maxBound

mkEsQueueSize :: Int -> Maybe EsQueueSize
mkEsQueueSize = mkNonZero EsQueueSize

-------------------------------------------------------------------------------
newtype EsPoolSize = EsPoolSize {
      unEsPoolSize :: Int
    } deriving (Show, Eq, Ord)

instance Bounded EsPoolSize where
  minBound = EsPoolSize 1
  maxBound = EsPoolSize maxBound

mkEsPoolSize :: Int -> Maybe EsPoolSize
mkEsPoolSize = mkNonZero EsPoolSize

-------------------------------------------------------------------------------
mkNonZero :: (Int -> a) -> Int -> Maybe a
mkNonZero ctor n
  | n > 0     = Just $ ctor n
  | otherwise = Nothing

-------------------------------------------------------------------------------
timeoutInMicroSeconds
  :: Seconds
  -> MicroSeconds
  -> Int
timeoutInMicroSeconds (Seconds s) (MicroSeconds ms) = (s * 1000000) + ms

-------------------------------------------------------------------------------
mkTimeout
  :: Timeout
  -> IO (TVar Bool)
mkTimeout (Timeout s ms) = registerDelay $ timeoutInMicroSeconds s ms
mkTimeout (TimeoutExt t) = return t

-------------------------------------------------------------------------------
cancelTimeout
  :: Timeout
  -> IO ()
cancelTimeout (Timeout _ _) = return ()
cancelTimeout (TimeoutExt t) = atomically $ writeTVar t False

-------------------------------------------------------------------------------
startWorker
    :: EsScribeCfg
    -> BHEnv
    -> MappingName
    -> TBMQueue (IndexName, Value)
    -> IO ()
startWorker EsScribeCfg {..} env mapping q = go
  where
    go = do
      popped <- atomically $ readTBMQueue q
      case popped of
        Just (ixn, v) -> do
          sendLog ixn v `catchAny` eat
          go
        Nothing -> do
          report' "Single Logger closing"
          return ()
    sendLog :: IndexName -> Value -> IO ()
    sendLog ixn v = void $ recovering essRetryPolicy [handler] $ const $ do
      did <- mkDocId
      res <- runBH env $ indexDocument ixn mapping defaultIndexDocumentSettings v did
      return res
    eat _ = return ()
    handler _ = Handler $ \e ->
      case fromException e of
        Just (_ :: AsyncException) -> return False
        _ -> return True
    report' text = liftIO $ report essDebugCallback text

timeIt :: IO a -> IO (a, Double)
timeIt action = do
    startTime <- getPOSIXTime
    result <- action
    endTime <- getPOSIXTime
    let duration = toSeconds $ endTime - startTime
    return (result, duration)

toSeconds :: POSIXTime -> Double
toSeconds = fromRational . toRational


startBulkWorker
    :: EsScribeCfg
    -> BHEnv
    -> BulkSendCfg
    -- ^ What is the bulking strategy
    -> MappingName
    -> TBMQueue (IndexName, Value)
    -> IO ()
startBulkWorker EsScribeCfg {..} env (BulkSendCfg {..}) mapping q = do
  stopSignal <- newTVarIO False
  acc <- newTVarIO []

  let popAction = readTBMQueue q
  -- Start an action that will read from our queue into an accumulation
  -- variable to bulk up values
  _ <- async $ do
    let act = do
          v <- atomically $ popAction
          case v of
            -- If the queue has been closed and is empty we want to signal that its all over
            Nothing -> do
              atomically $ writeTVar stopSignal True
              return False
            -- Accumulate the value and continue
            Just v' -> do
              atomically $ modifyTVar' acc (v':)
              return True
    -- If we have been told not to continue stop recursing
    let act' = act >>= \x -> if x
          then yield >> (threadDelay $ unMicroSeconds bscAccumulationDelay) >> act'
          else return ()
    act'

  -- Start the recursive worker
  go stopSignal acc
  where
    go stopSignal acc = do
      let popPred = mkSendPredicate bscBulkSendType

      timedOut <- mkTimeout bscTimeout


      -- Wait until the timeout is true
      let waitTimeout = do
            x <- readTVar timedOut
            when (not x) retry

      -- Wait until the predicate is false
      let waitEls = do
            es <- readTVar acc
            let cont = popPred es
            when cont $ retry

      -- If we are set to debug, send a signal to the configured signal var that we are starting a wait
      signal' $ DSStartWait
      -- Wait on timeout or predicate to stop retrying, return all the accumulated values
      (popped, duration) <- timeIt $ atomically $ do
        waitTimeout <|> waitEls
        swapTVar acc []
      -- Cleanup the timeout
      cancelTimeout bscTimeout
      -- If we are set to debug, send a signal to the configured signal var that we are finished waiting
      signal' $ DSFinishWait
      signal' $ DSTimeTaken duration


      -- If we have values to send, bulk them up and send them
      when (length popped > 0) $ do
        (_, sendDuration) <- timeIt $ do
          let bulkOp ixn v = mkDocId >>= \did -> pure $ BulkIndex ixn mapping did v
          bulkOps <- V.fromList <$> mapM (\(ixn, v) -> bulkOp ixn v) popped
          sendLog bulkOps `catchAny` eat
        signal' $ DSSendTimeTaken sendDuration
        -- If we are set to debug, send a signal for how many items were sent
        signal' $ DSSent $ length popped

      -- Is the queue closed?
      stop <- atomically $ readTVar stopSignal

      if stop
        then do
          report' "Bulk Logger closing"
          pure ()
        else go stopSignal acc

    sendLog :: V.Vector BulkOperation -> IO ()
    sendLog ops = void $ recovering essRetryPolicy [handler] $ const $ do
      res <- runBH env $ bulk ops
      pure res
    eat _ = pure ()
    handler _ = Handler $ \e ->
      case fromException e of
        Just (_ :: AsyncException) -> return False
        _ -> return True

    report' text = liftIO $ report essDebugCallback text
    signal' i = liftIO $ signal essDebugCallback i

mkSendPredicate
    :: BulkSendType
    -> [(IndexName, Value)]
    -> Bool
mkSendPredicate (SendThresholdCount i) = go
  where
    go !ac = if (length ac) >= i
      then False
      else True
mkSendPredicate (SendThresholdPredicate p) = p
