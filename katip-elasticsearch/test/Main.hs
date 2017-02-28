{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell     #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module Main
    ( main
    ) where


-------------------------------------------------------------------------------
import           Control.Applicative         as A
import           Control.Concurrent
import           Control.Concurrent.Async
import           Control.Concurrent.STM
import           Control.Concurrent.STM.TBChan
import           Control.Lens                hiding (mapping, (.=))
import           Control.Monad
import           Control.Monad.IO.Class
import           Data.Aeson
import           Data.Aeson.Lens
import           Data.Aeson.Types
import qualified Data.HashMap.Strict         as HM
import qualified Data.Map                    as M
import           Data.Monoid
import           Data.Scientific
import qualified Data.Text.IO                as TIO
import           Data.Time
import           Data.Time.Calendar.WeekDate
import qualified Data.Vector                 as V
import           Database.Bloodhound         hiding (key, Status)
import           Network.HTTP.Client
import           Network.HTTP.Types.Status
import           Test.QuickCheck.Instances   ()
import           Test.Tasty
import           Test.Tasty.HUnit
import           Test.Tasty.QuickCheck
-------------------------------------------------------------------------------
import           Katip
import           Katip.Scribes.ElasticSearch
-------------------------------------------------------------------------------


main :: IO ()
main = defaultMain $ testGroup "katip-elasticsearch"
  [
    esTests
  , typeAnnotatedTests
  , roundToSundayTests
  ]


-------------------------------------------------------------------------------
setupSearch :: (EsScribeCfg -> EsScribeCfg) -> IO (Scribe, IO ())
setupSearch modScribeCfg = do
    bh dropESSchema
    mgr <- newManager defaultManagerSettings
    mkEsScribe cfg (mkBHEnv svr mgr) ixn mn DebugS V3
  where
    cfg = modScribeCfg (defaultEsScribeCfg { essAnnotateTypes = True
                                           , essIndexSettings = ixs
                                           })

-------------------------------------------------------------------------------
setupSearchIO :: (EsScribeCfg -> IO EsScribeCfg) -> IO (Scribe, IO ())
setupSearchIO modScribeCfg = do
    bh dropESSchema
    mgr <- newManager defaultManagerSettings
    cfg <- modScribeCfg (defaultEsScribeCfg { essAnnotateTypes = True
                                            , essIndexSettings = ixs
                                            })
    mkEsScribe cfg (mkBHEnv svr mgr) ixn mn DebugS V3


-------------------------------------------------------------------------------
teardownSearch :: (Scribe, IO ()) -> IO ()
teardownSearch (_, finalizer) = do
  finalizer
  bh $ do
    when False $ dropESSchema
    when False $ dropESSTemplate --TODO: drop


-------------------------------------------------------------------------------
withSearch :: (IO (Scribe, IO ()) -> TestTree) -> TestTree
withSearch = withSearch' id


-------------------------------------------------------------------------------
withSearch' :: (EsScribeCfg -> EsScribeCfg) -> (IO (Scribe, IO ()) -> TestTree) -> TestTree
withSearch' modScribeCfg = withResource (setupSearch modScribeCfg) teardownSearch

-------------------------------------------------------------------------------
withSearchIO :: (EsScribeCfg -> IO EsScribeCfg) -> (IO (Scribe, IO ()) -> TestTree) -> TestTree
withSearchIO modScribeCfg = withResource (setupSearchIO modScribeCfg) teardownSearch


-------------------------------------------------------------------------------
esTests :: TestTree
esTests = testGroup "elasticsearch scribe"
  [
    withSearch' (\c -> c { essIndexSharding = NoIndexSharding}) $ \setup -> testCase "it flushes to elasticsearch" $ withTestLogging setup $ \done -> do
       $(logT) (ExampleCtx True) mempty InfoS "A test message"
       liftIO $ do
         void done
         logs <- getLogs
         length logs @?= 1
         let l = head logs
         l ^? key "_source" . key "msg" . _String @?= Just "A test message"
         l ^? key "_source" . key "data" . key "whatever::b" . _Bool @?= Just True
  , withSearch $ \setup -> testCase "date-based index sharding" $ do
      let t1 = mkTime 2016 1 2 3 4 5
      fakeClock <- newTVarIO t1
      withTestLogging' (set logEnvTimer (readTVarIO fakeClock)) setup $ \done -> do
        $(logT) (ExampleCtx True) mempty InfoS "today"
        let t2 = mkTime 2016 1 3 3 4 5
        liftIO (atomically (writeTVar fakeClock t2))
        $(logT) (ExampleCtx True) mempty InfoS "tomorrow"
        liftIO $ do
          void done
          todayLogs <- getLogsByIndex (IndexName "katip-elasticsearch-tests-2016-01-02")
          tomorrowLogs <- getLogsByIndex (IndexName "katip-elasticsearch-tests-2016-01-03")
          assertBool ("todayLogs has " <> show (length todayLogs) <> " items") (length todayLogs == 1)
          assertBool ("tomorrowLogs has " <> show (length tomorrowLogs) <> " items") (length tomorrowLogs == 1)
          let logToday = head todayLogs
          let logTomorrow = head tomorrowLogs
          logToday ^? key "_source" . key "msg" . _String @?= Just "today"
          logTomorrow ^? key "_source" . key "msg" . _String @?= Just "tomorrow"
  , withSearch' (\c -> c { essIndexSharding = WeeklyIndexSharding}) $ \setup -> testCase "weekly index sharding rounds to previous sunday" $ do
      let t1 = mkTime 2016 3 5 0 0 0 -- saturday, march 5th
      fakeClock <- newTVarIO t1
      withTestLogging' (set logEnvTimer (readTVarIO fakeClock)) setup $ \done -> do
        $(logT) (ExampleCtx True) mempty InfoS "today"
        let t2 = mkTime 2016 3 6 0 0 0 -- sunday march 6th
        liftIO (atomically (writeTVar fakeClock t2))
        $(logT) (ExampleCtx True) mempty InfoS "tomorrow"
        liftIO $ do
          void done
          todayLogs <- getLogsByIndex (IndexName "katip-elasticsearch-tests-2016-02-28") -- rounds back to previous sunday
          tomorrowLogs <- getLogsByIndex (IndexName "katip-elasticsearch-tests-2016-03-06") -- is on sunday, so uses current date
          assertBool ("todayLogs has " <> show (length todayLogs) <> " items") (length todayLogs == 1)
          assertBool ("tomorrowLogs has " <> show (length tomorrowLogs) <> " items") (length tomorrowLogs == 1)
          let logToday = head todayLogs
          let logTomorrow = head tomorrowLogs
          logToday ^? key "_source" . key "msg" . _String @?= Just "today"
          logTomorrow ^? key "_source" . key "msg" . _String @?= Just "tomorrow"
  , let Just ps = mkEsPoolSize 1
    in
      withResource (newTVarIO False) (const $ return ()) $ \timeoutVar' ->
      withResource (newTBChanIO 1) (const $ return ()) $ \tSig' ->
      withSearchIO (\c -> do timeoutVar <- timeoutVar'
                             tSig <- tSig'
                             return $ c { essQueueSendThreshold = BulkSend $ BulkSendCfg (TimeoutExt timeoutVar) (SendThresholdCount 3) (MicroSeconds 100) NoAsyncSend
                                        , essLoggingGuarantees = Try 5
                                        , essDebugCallback = DebugCallback (Just $ TIO.putStrLn) (Just $ (SignalBlock, tSig)) 100000000
                                        , essPoolSize = ps
                                        }
                  ) $ \setup -> testCase "test bulk sending" $ do
        let t1 = mkTime 2016 1 2 3 4 5
        fakeClock <- newTVarIO t1
        timeoutVar <- timeoutVar'
        tSig <- tSig'

        let waitForSignal sig =
              atomically $ do
                sent <- readTBChan tSig
                unless (sent == sig) retry
            waitForSignalIsh sigEq =
              atomically $ do
                sent <- readTBChan tSig
                unless (sigEq sent) retry
            forceTimeout =
              threadDelay 100 >> (atomically $ writeTVar timeoutVar True)

        withTestLogging' (set logEnvTimer (readTVarIO fakeClock)) setup $ \done -> do
          $(logT) (ExampleCtx True) mempty InfoS "today"
          let t2 = mkTime 2016 1 3 3 4 5
          liftIO (atomically (writeTVar fakeClock t2))
          $(logT) (ExampleCtx True) mempty InfoS "tomorrow"
          -- Test that a timeout will let a smaller number than bulk configured get sent
          liftIO $ do
            waitForSignal DSStartWait
            forceTimeout
            waitForSignal DSFinishWait
            waitForSignalIsh (\x -> case x of
                             DSTimeTaken {} -> True
                             _ -> False
                             )
            waitForSignal DSStartSend
            waitForSignal DSFinishSend
            waitForSignalIsh (\x -> case x of
                             DSSendTimeTaken {} -> True
                             _ -> False
                             )
            waitForSignal $ DSSent 2
            void $ bh (refreshIndex ixn)
            todayLogs <- getLogsByIndex (IndexName "katip-elasticsearch-tests-2016-01-02")
            assertBool ("todayLogs has " <> show (length todayLogs) <> " items") (length todayLogs == 1)
            tomorrowLogs <- getLogsByIndex (IndexName "katip-elasticsearch-tests-2016-01-03")
            assertBool ("tomorrowLogs has " <> show (length tomorrowLogs) <> " items") (length tomorrowLogs == 1)
            getLogsByIndexWithStatus_ (IndexName "katip-elasticsearch-tests-2016-01-04") status404
          let t3 = mkTime 2016 1 4 3 4 5
          liftIO (atomically (writeTVar fakeClock t3))
          $(logT) (ExampleCtx True) mempty InfoS "nextTomorrow"
          $(logT) (ExampleCtx True) mempty InfoS "nextTomorrow"
          $(logT) (ExampleCtx True) mempty InfoS "nextTomorrow"
          -- Test that a bulk configured get sent
          liftIO $ do
            waitForSignal DSStartWait
            waitForSignal DSFinishWait
            waitForSignalIsh (\x -> case x of
                             DSTimeTaken {} -> True
                             _ -> False
                             )
            waitForSignal DSStartSend
            waitForSignal DSFinishSend
            waitForSignalIsh (\x -> case x of
                             DSSendTimeTaken {} -> True
                             _ -> False
                             )
            waitForSignal $ DSSent 3
            void $ bh (refreshIndex ixn)
            todayLogs <- getLogsByIndex (IndexName "katip-elasticsearch-tests-2016-01-02")
            tomorrowLogs <- getLogsByIndex (IndexName "katip-elasticsearch-tests-2016-01-03")
            nextTomorrowLogs <- getLogsByIndex (IndexName "katip-elasticsearch-tests-2016-01-04")
            assertBool ("todayLogs has " <> show (length todayLogs) <> " items") (length todayLogs == 1)
            assertBool ("tomorrowLogs has " <> show (length tomorrowLogs) <> " items") (length tomorrowLogs == 1)
            assertBool ("nextTomorrowLogs has " <> show (length nextTomorrowLogs) <> " items") (length nextTomorrowLogs == 3)
            let logToday = head todayLogs
            let logTomorrow = head tomorrowLogs
            let logNextTomorrow = head nextTomorrowLogs
            logToday ^? key "_source" . key "msg" . _String @?= Just "today"
            logTomorrow ^? key "_source" . key "msg" . _String @?= Just "tomorrow"
            logNextTomorrow ^? key "_source" . key "msg" . _String @?= Just "nextTomorrow"
            _ <- async $ do
              waitForSignal DSStartWait
              forceTimeout
              waitForSignal DSFinishWait
              waitForSignalIsh (\x -> case x of
                               DSTimeTaken {} -> True
                               _ -> False
                               )
            void $ done
  ]


-------------------------------------------------------------------------------
mkTime :: Integer -> Int -> Int -> DiffTime -> DiffTime -> DiffTime -> UTCTime
mkTime y m d hr minute s = UTCTime day dt
  where
    day = mkDay y m d
    dt = s + 60 * minute + 60 * 60 * hr


-------------------------------------------------------------------------------
mkDay :: Integer -> Int -> Int -> Day
mkDay y m d = day
  where
    Just day = fromGregorianValid y m d


-------------------------------------------------------------------------------
data ExampleCtx = ExampleCtx {
      ecBool :: Bool
    }

instance ToJSON ExampleCtx where
  toJSON c = object ["whatever" .= ecBool c]


instance ToObject ExampleCtx


instance LogItem ExampleCtx where
  payloadKeys _ _ = AllKeys

-------------------------------------------------------------------------------
typeAnnotatedTests :: TestTree
typeAnnotatedTests = testGroup "TypeAnnotated"
  [
    testCase "annotates values on toJSON" $
      toJSON (TypeAnnotated exampleValue) @?= annotatedExampleValue

  , testCase "annotates values on toObject" $
      toObject (TypeAnnotated exampleObject) @?= annotatedExampleObject

  , testCase "deannotates on parseJSON" $
      parseEither parseJSON (toJSON exampleValue) @?= Right exampleValue

  , testProperty "roundtrips the same as raw" $ \(v :: Value) ->
      let res = typeAnnotatedValue
                <$> parseEither parseJSON (toJSON (TypeAnnotated v))
      in res === Right v
  ]


-------------------------------------------------------------------------------
roundToSundayTests :: TestTree
roundToSundayTests = testGroup "roundToSunday"
  [
    testProperty "always returns a sunday" $ \d ->
      getDOW (roundToSunday d) === 7
  , testProperty "returns input on sunday" $ \d -> getDOW d == 7 ==>
      roundToSunday d === d
  , testProperty "goes back a week when not sunday" $ \d -> getDOW d /= 7 ==>
      roundToSunday d < d
  ]
  where
    getDOW = view _3 . toWeekDate


-------------------------------------------------------------------------------
exampleObject :: Object
exampleObject = HM.fromList
  [ ("a bool", Bool False)
  , ("a long", Number 24)
  , ("a double", Number 52.3)
  , ("a string", String "s")
  , ("a null", Null)
  , ("a map", Object (HM.singleton "baz" (Bool True)))
  ]


-------------------------------------------------------------------------------
annotatedExampleObject :: Object
annotatedExampleObject = HM.fromList
  [ ("a map",Object $ HM.fromList [("baz::b", Bool True)])
  , ("a bool::b", Bool False)
  , ("a null::n", Null)
  , ("a string::s", String "s")
  , ("a double::d", Number 52.3)
  , ("a long::l", Number 24.0)
  ]


-------------------------------------------------------------------------------
exampleValue :: Value
exampleValue = Array $ V.fromList [Null, Object exampleObject]


-------------------------------------------------------------------------------
annotatedExampleValue :: Value
annotatedExampleValue = Array $ V.fromList
  [ Null
  , Object annotatedExampleObject
  ]


-------------------------------------------------------------------------------
getLogs :: IO [Value]
getLogs = getLogsByIndex ixn


-------------------------------------------------------------------------------
getLogsByIndex :: IndexName -> IO [Value]
getLogsByIndex i = do
  r <- bh $ do
    void (refreshIndex i)
    searchByIndex i (mkSearch Nothing Nothing)
  let actualCode = statusCode (responseStatus r)
  assertBool ("search by " <> show i <> " " <> show actualCode <> " /= 200") (actualCode == 200)
  return $ responseBody r ^.. key "hits" . key "hits" . values

getLogsByIndexWithStatus_ :: IndexName -> Status -> IO ()
getLogsByIndexWithStatus_ i s = do
  r <- bh $ do
    void (refreshIndex i)
    searchByIndex i (mkSearch Nothing Nothing)
  let actualCode = statusCode (responseStatus r)
  assertBool ("search by " <> show i <> " " <> show actualCode <> " /= " <> show (statusCode s)) (actualCode == (statusCode s))
  return ()

-------------------------------------------------------------------------------
bh :: BH IO a -> IO a
bh = withBH defaultManagerSettings svr


-------------------------------------------------------------------------------
withTestLogging
  :: IO (Scribe, IO a) -> (IO Reply -> KatipT IO b) -> IO b
withTestLogging = withTestLogging' id


-------------------------------------------------------------------------------
withTestLogging'
  :: (LogEnv -> LogEnv)
  -> IO (Scribe, IO a)
  -> (IO Reply -> KatipT IO b)
  -> IO b
withTestLogging' modEnv setup f = do
  (scr, done) <- setup
  le <- modEnv <$> initLogEnv ns env
  let done' = done >> bh (refreshIndex ixn)
  runKatipT le { _logEnvScribes = M.singleton "es" scr} (f done')
  where
    ns = Namespace ["katip-test"]
    env = Environment "test"


-------------------------------------------------------------------------------
svr :: Server
svr = Server "http://localhost:9200"


-------------------------------------------------------------------------------
ixn :: IndexName
ixn = IndexName "katip-elasticsearch-tests"


-------------------------------------------------------------------------------
ixs :: IndexSettings
ixs = defaultIndexSettings { indexShards = ShardCount 1
                           , indexReplicas = ReplicaCount 1}

-------------------------------------------------------------------------------
mn :: MappingName
mn = MappingName "logs"


-------------------------------------------------------------------------------
dropESSchema :: BH IO ()
dropESSchema = void $ deleteIndex (IndexName "katip-elasticsearch-tests*")


-------------------------------------------------------------------------------
dropESSTemplate :: BH IO ()
dropESSTemplate = void $ deleteTemplate (TemplateName "katip-elasticsearch-tests")


-------------------------------------------------------------------------------
instance Arbitrary Value where
  arbitrary = oneof
    [ Object <$> reduceSize arbitrary
    , Array . V.fromList <$> reduceSize arbitrary
    , String <$> arbitrary
    , Number <$> (scientific <$> arbitrary <*> arbitrary)
    , Bool <$> arbitrary
    , A.pure Null
    ]


-------------------------------------------------------------------------------
-- | Reduce the size of Arbitrary input for the given generator
reduceSize :: Gen a -> Gen a
reduceSize f = sized $ \ n -> resize (n `div` 2) f
