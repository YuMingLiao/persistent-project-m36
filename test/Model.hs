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
{-# LANGUAGE DerivingStrategies         #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
--{-# LANGUAGE DeriveAnyClass             #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE StandaloneDeriving         #-}
module Model where
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
    
-- `age Int Maybe` are not supported becuase PersistNull can't be turned into a legal AtomType for now.
mkPersist projectM36Settings [persistLowerCase|
Person
    name Text 
    age Int 
    deriving Show Generic 
BlogPost
    title Text 
    authorId PersonId
    deriving Show Generic 
|]

deriving instance Generic (Key Person)
deriving instance Generic (Key BlogPost)


