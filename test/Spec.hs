{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# LANGUAGE EmptyDataDecls             #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE GADTs                      #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE QuasiQuotes                #-}
{-# LANGUAGE TemplateHaskell            #-}
{-# LANGUAGE TypeFamilies               #-}
{-# LANGUAGE TypeSynonymInstances       #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE DeriveAnyClass             #-}
{-# LANGUAGE DerivingStrategies         #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE StandaloneDeriving         #-}
import           Control.Monad.IO.Class  (liftIO)
import           Database.Persist
import           Database.Persist.ProjectM36 hiding (executeDatabaseContextExpr)
import           Database.Persist.TH
import           ProjectM36.Client 
import           Data.Either
import           Control.Monad.Reader
import           Data.Proxy
import           ProjectM36.Tupleable
import           GHC.Generics
import           Data.Text
import qualified Control.DeepSeq as N
import qualified Data.Binary as B
import Data.UUID.V4 (nextRandom)
import qualified Data.UUID as U
import qualified Data.Text as T
import Model

-- these deriving have conflict with GeneralizedNewTypeDeriving in Model.hs, so they are put here.
deriving anyclass instance Tupleable Person
deriving anyclass instance Tupleable BlogPost 
deriving anyclass instance B.Binary (Key Person)
deriving anyclass instance N.NFData (Key Person)
deriving anyclass instance Atomable (Key Person)

handleIOError :: Show e => IO (Either e a) -> IO a
handleIOError m = do
  v <- m
  handleError v
    
handleError :: Show e => Either e a -> IO a
handleError eErr = case eErr of
    Left err -> print err >> error "Died due to errors."
    Right v -> pure v
    
handleIOErrors :: Show e => IO [Either e a] -> IO [a]
handleIOErrors m = do
  eErrs <- m
  case lefts eErrs of
    [] -> pure (rights eErrs)    
    err:_ -> handleError (Left err)

main :: IO ()
main = do
  --connect to the database
  let connInfo = InProcessConnectionInfo NoPersistence emptyNotificationCallback []
  _ <- connectProjectM36 connInfo
  conn <- handleIOError $ connectProjectM36 connInfo
  sessionId <- handleIOError $ createSessionAtHead conn "master"
  flip runProjectM36Conn (sessionId, conn) $ do
    (sessionId,conn) <- ask
    liftIO $ createSchema sessionId conn  

    johnId <- insert $ Person "John Doe" 35
    janeId <- insert $ Person "Jane Doe" 2

    insert $ BlogPost "My fr1st p0st" johnId
    insert $ BlogPost "One more for good measure" johnId

    oneJohnPost <- selectList [BlogPostAuthorId ==. johnId] [] -- [LimitTo 1] not supported yet
--    liftIO $ print (oneJohnPost :: [Entity BlogPost])

    john <- get johnId
--    liftIO $ print (john :: Maybe Person)

    delete janeId
    deleteWhere [BlogPostAuthorId ==. johnId]

createSchema :: SessionId -> Connection -> IO ()
createSchema sessionId conn = do
  freshUUID <- nextRandom
  toDefinePersonExpr <- handleError $ toDefineExprWithId (Person "Test" 0) "person"
  toDefineBlogPostExpr <- handleError $ toDefineExprWithId (BlogPost "Test" (PersonKey (ProjectM36Key freshUUID))) "blog_post"
  _ <- handleIOErrors $ mapM (executeDatabaseContextExpr sessionId conn) [
    toDefinePersonExpr,
    toDefineBlogPostExpr,
    databaseContextExprForForeignKey "blog_post__person" ("blog_post", ["author_id"]) ("person", ["id"]),
    databaseContextExprForUniqueKey "person" ["id"]
    ]
  pure ()


-- for debug
--  print $ entityDef $ Just (Person "asdfasd" (Just 32))
