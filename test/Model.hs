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
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE StandaloneDeriving         #-}
module Model where
import           Database.Persist
import           Database.Persist.ProjectM36 hiding (executeDatabaseContextExpr)
import           Database.Persist.TH
import           GHC.Generics
import           Data.Text
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


