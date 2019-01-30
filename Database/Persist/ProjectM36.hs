{-# LANGUAGE TemplateHaskell, OverloadedStrings, RankNTypes, TypeFamilies, FlexibleInstances, FlexibleContexts, StandaloneDeriving, DeriveAnyClass, DeriveGeneric, ScopedTypeVariables #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module Database.Persist.ProjectM36 where
import Database.Persist hiding (Assign, Update)
import qualified Database.Persist as DP
import Database.Persist.TH  
import Language.Haskell.TH (Type(ConT))
import ProjectM36.Base
import ProjectM36.Tuple
import qualified ProjectM36.Client as C
import qualified ProjectM36.DataFrame as DF
import qualified Data.UUID as U
import Data.UUID.V4 (nextRandom)
import qualified Data.Vector as V
import ProjectM36.Error
import ProjectM36.Relation
import ProjectM36.AtomFunctions.Primitive
import ProjectM36.DataTypes.Primitive
import qualified Control.Monad.IO.Class as Trans
import Control.Monad.Trans.Class
import Control.Monad.Trans.Reader (ask, ReaderT)
import Control.Exception (throw, throwIO)
import qualified Data.Text as T
import ProjectM36.Atom 
import Data.Aeson (FromJSON(..), ToJSON(..), withText, withObject, (.:))
import qualified Data.Map as M
import Control.Monad (void, unless)
import Data.Aeson.Types (modifyFailure)
import Control.Monad.Reader (runReaderT)
import Web.PathPieces (PathPiece (..))
import qualified Database.Persist.Sql as Sql
import Control.Monad.Trans.Except
import qualified Database.Persist.Types as DPT
import qualified Data.Set as S
import qualified Data.Conduit.List as CL
import Web.HttpApiData (ToHttpApiData(..), FromHttpApiData(..), parseUrlPieceWithPrefix, readTextData)
import Data.Either (isRight)
import Control.Error.Util (hoistEither)
import GHC.Generics
import Control.DeepSeq
import Data.Binary
import Data.Maybe

projectM36Settings :: MkPersistSettings
projectM36Settings = mkPersistSettings $ ConT ''ProjectM36Backend

instance HasPersistBackend ProjectM36Backend where
    type BaseBackend ProjectM36Backend = ProjectM36Backend
    persistBackend = id


type ProjectM36Backend = (C.SessionId, C.Connection) 

--convert a PersistEntity to a RelationTuple
recordAsTuple :: forall record. (PersistEntity record)
            => Maybe U.UUID -> record -> Either RelationalError RelationTuple
recordAsTuple uuid record = do -- if the uuid is passed in, set the idDBName attribute
  let entInfo = entityInfo record
  attrVec <- recordAttributes entInfo record
  atomVec <- recordAsAtoms uuid attrVec record
  return $ RelationTuple attrVec atomVec

recordAttributes :: forall record. (PersistEntity record)
           => [(EmbedFieldDef, PersistValue)] -> record -> Either RelationalError Attributes
recordAttributes entInfo record = do
  let convertAttr = uncurry fieldDefAsAttribute
      idDBName = unDBName $ fieldDB $ entityId (entityDef $ Just record)
  attrList <- mapM convertAttr entInfo
  return $ V.fromList (Attribute idDBName TextAtomType : attrList)

fieldDefAsAttribute :: EmbedFieldDef -> PersistValue -> Either RelationalError Attribute
fieldDefAsAttribute fieldDef pVal = case persistValueAtomType pVal of
  Nothing -> Left $ AtomTypeNotSupported attrName
  Just fType -> Right $ Attribute attrName fType
  where
    attrName = unDBName $ emFieldDB fieldDef

persistValueAtomType :: PersistValue -> Maybe AtomType
persistValueAtomType val = do
  atom <- persistValueAtom val
  return $ atomTypeForAtom atom

persistValueAtom :: PersistValue -> Maybe Atom
persistValueAtom val {- | traceShow val True -} = case val of
  PersistText v -> return $ TextAtom v
  PersistInt64 v -> return $ IntAtom (fromIntegral v :: Int)
  PersistBool v -> return $ BoolAtom v
  PersistDay v -> return $ DayAtom v
  _ -> Nothing

atomAsPersistValue :: Atom -> PersistValue
--constructed atoms are written as text to the database
atomAsPersistValue atom@ConstructedAtom{} = PersistText (atomToText atom)
atomAsPersistValue (IntAtom i) = PersistInt64 (fromIntegral i)
atomAsPersistValue (DoubleAtom i) = PersistDouble i
atomAsPersistValue (TextAtom i) = PersistText i
atomAsPersistValue (DayAtom i) = PersistDay i
atomAsPersistValue (DateTimeAtom i) = PersistUTCTime i
atomAsPersistValue (ByteStringAtom i) = PersistByteString i
atomAsPersistValue (BoolAtom i) = PersistBool i
atomAsPersistValue _ = error "Persist: missing conversion"

recordAsAtoms :: forall record. (PersistEntity record)
           => Maybe U.UUID -> Attributes -> record -> Either RelationalError (V.Vector Atom)
recordAsAtoms freshUUID _ record = do
  let pValues = map toPersistValue $ toPersistFields record
      valAtom val = case persistValueAtom val of
        Nothing -> Left $ AtomTypeNotSupported ""
        Just atom -> Right atom
  atoms <- mapM valAtom pValues
  return $ V.fromList $ case freshUUID of
    Nothing -> atoms
    Just uuid -> TextAtom (T.pack (U.toString uuid)) : atoms

--used by insert operations
recordsAsRelation :: forall record. (PersistEntity record) =>
                     [(Maybe U.UUID, record)] -> Either RelationalError Relation
recordsAsRelation [] = Left EmptyTuplesError
recordsAsRelation recordZips = do
  tupleList <- mapM (uncurry recordAsTuple) recordZips
  let oneTuple' = head tupleList
  mkRelation (tupleAttributes oneTuple') (RelationTupleSet tupleList)

keyFromValuesOrDie :: (Trans.MonadIO m,
                       PersistEntity record) => [PersistValue] -> m (Key record)
keyFromValuesOrDie val = case keyFromValues val of
  Right k -> return k
  Left _ -> Trans.liftIO $ throwIO $ PersistError "key pooped"

insertNewRecords :: (Trans.MonadIO m,
                     PersistEntity record) =>
                    [U.UUID] -> [record] -> ReaderT ProjectM36Backend m [Key record]
insertNewRecords uuids records = do
  let recordsZip = zip (map Just uuids) records
      insertExpr = Insert (relVarNameFromRecord $ head records)
  case recordsAsRelation recordsZip of
      Left err -> throw $ PersistError (T.pack $ show err)
      Right rel -> do
        (sessionId, conn) <- ask
        Trans.liftIO $ executeDatabaseContextExpr sessionId conn (insertExpr (ExistingRelation rel))
        mapM (\uuid -> keyFromValuesOrDie [PersistText $ T.pack (U.toString uuid)]) uuids

throwIOPersistError :: (Trans.MonadIO m) => String -> m a
throwIOPersistError msg = Trans.liftIO $ throwIO $ PersistError (T.pack msg)

lookupByKey :: forall record m.
               (Trans.MonadIO m,
                PersistEntity record)
           => Key record -> ReaderT ProjectM36Backend m (Maybe (Entity record))
lookupByKey key = do
  (relVarName, restrictionPredicate) <- commonKeyQueryProperties key
  let query = Restrict restrictionPredicate (RelationVariable relVarName ())
  (sessionId, conn) <- ask
  resultRel <- Trans.liftIO $ C.executeRelationalExpr sessionId conn query
  case resultRel of
    Left err -> throwIOPersistError ("executeRelationExpr error: " ++ show err)
    Right resultRel' -> case singletonTuple resultRel' of
      Nothing -> return Nothing --no match on key
      Just tuple -> do
        let entityInfo' = entityDefFromKey key
        entity <- fromPersistValuesThrow entityInfo' tuple --correct, one match
        return $ Just entity

--supports processing of either RelationTuple or DataFrameTuple
class DBTuple t where
  tupAtomForAttributeName :: AttributeName -> t -> Either RelationalError Atom

instance DBTuple RelationTuple where
  tupAtomForAttributeName = atomForAttributeName

instance DBTuple DF.DataFrameTuple where
  tupAtomForAttributeName = DF.atomForAttributeName

fromPersistValuesThrow :: (Trans.MonadIO m,
                           PersistEntity record,
                           DBTuple tup) => EntityDef -> tup -> m (Entity record)
fromPersistValuesThrow entDef tuple = do
  let body = fromPersistValues $ map getValue (entityFields entDef)
      getValue field = case tupAtomForAttributeName (unDBName (fieldDB field)) tuple of
        Right atom -> atomAsPersistValue atom
        Left err -> throw $ PersistError ("missing field: " `T.append` T.pack (show err))
      keyAtom = tupAtomForAttributeName (unDBName (fieldDB (entityId entDef))) tuple
  case keyAtom of
    Left err -> throwIOPersistError ("missing key atom" ++ show err)
    Right keyAtom' -> case keyFromValues [atomAsPersistValue keyAtom'] of
      Left err -> throw $ PersistError ("key failure" `T.append` T.pack (show err))
      Right key -> case body of
        Left err -> throwIOPersistError (show err)
        Right body' -> return $ Entity key body'

commonKeyQueryProperties :: (Trans.MonadIO m, PersistEntity val) => Key val -> ReaderT ProjectM36Backend m (RelVarName, RestrictionPredicateExpr)
commonKeyQueryProperties key = do
  let matchUUID = keyToUUID key
      keyAttributeName = unDBName (fieldDB $ entityId entityInfo')
      relVarName = unDBName $ entityDB entityInfo'
      matchUUIDText = T.pack $ U.toString matchUUID
      entityInfo' = entityDefFromKey key
      restrictionPredicate = AttributeEqualityPredicate keyAttributeName (NakedAtomExpr (TextAtom matchUUIDText))
  return (relVarName, restrictionPredicate)

deleteByKey :: (Trans.MonadIO m, PersistEntity val) => Key val -> ReaderT ProjectM36Backend m Bool
deleteByKey key = do
  (relVarName, restrictionPredicate) <- commonKeyQueryProperties key
  let query = Delete relVarName restrictionPredicate
  (sessionId, conn) <- ask
  eiRes <- Trans.liftIO $ C.executeDatabaseContextExpr sessionId conn query
  return $ isRight eiRes

--convert persistent update list to ProjectM36 Update map
updatesToUpdateMap :: (Trans.MonadIO m,
                       PersistEntity val) =>
                      [DP.Update val] -> ReaderT ProjectM36Backend m (M.Map AttributeName AtomExpr)
updatesToUpdateMap updates = do
  let convertMap upd = case updateUpdate upd of
        DP.Assign -> let attrName = unDBName $ fieldDB $ updateFieldDef upd
                         newAttrValue = persistValueAtom (updatePersistValue upd) in
                     case newAttrValue of
                       Nothing -> Trans.liftIO $ throwIO $ PersistError "atom conversion failed"
                       Just newAttrValue' -> return (attrName, NakedAtomExpr newAttrValue')
        _ -> Trans.liftIO $ throwIO $ PersistError "update type not supported"
  updateAtomList <- mapM convertMap updates
  return $ M.fromList updateAtomList

updateByKey :: (Trans.MonadIO m, PersistEntity val) => Key val -> [DP.Update val] -> ReaderT ProjectM36Backend m ()
updateByKey key updates = do
  (relVarName, restrictionPredicate) <- commonKeyQueryProperties key
  updateMap <- updatesToUpdateMap updates
  let query = Update relVarName updateMap restrictionPredicate
  (sessionId, conn) <- ask
  eiRes <- Trans.liftIO $ C.executeDatabaseContextExpr sessionId conn query
  case eiRes of
    Left err -> throwIOPersistError ("executeDatabaseContextExpr error" ++ show err)
    Right _ -> return ()

--set the truth unconditionally
repsertByKey :: (Trans.MonadIO m, ProjectM36Backend ~ PersistEntityBackend val, PersistEntity val) => Key val -> val -> ReaderT ProjectM36Backend m ()
repsertByKey key val = do
  _ <- delete key
  insertKey key val

replaceByKey :: (Trans.MonadIO m, ProjectM36Backend ~ PersistEntityBackend val, PersistEntity val) => Key val -> val -> ReaderT ProjectM36Backend m ()
replaceByKey key val = do
  deletionSuccess <- deleteByKey key
  unless deletionSuccess (Trans.liftIO $ throwIO $ PersistError "entity missing during replace")
  insertKey key val

entityDefFromKey :: PersistEntity record => Key record -> EntityDef
entityDefFromKey = entityDef . Just . recordTypeFromKey

dummyFromKey :: Key record -> Maybe record
dummyFromKey = Just . recordTypeFromKey

recordTypeFromKey :: Key record -> record
recordTypeFromKey _ = error "dummyFromKey"

dummyFromUnique :: Unique v -> Maybe v
dummyFromUnique _ = Nothing

dummyFromFilter :: Filter v -> Maybe v
dummyFromFilter _ = Nothing

dummyFromFilters :: [Filter v] -> Maybe v
dummyFromFilters _ = Nothing

dummyFromUpdate :: DPT.Update v -> Maybe v
dummyFromUpdate _ = Nothing

keyToUUID:: (PersistEntity record) => Key record -> U.UUID
keyToUUID key = case keyToValues key of
  [PersistText uuidText] -> fromMaybe (error "error reading uuid from text in key construction") (U.fromString (T.unpack uuidText))
  _ -> error "unexpected persist value in key construction"

instance ToHttpApiData (BackendKey ProjectM36Backend) where
  toUrlPiece = T.pack . U.toString . unPM36Key
    
instance FromHttpApiData (BackendKey ProjectM36Backend) where    
  parseUrlPiece input = do
    s <- parseUrlPieceWithPrefix "o" input <!> return input
    ProjectM36Key <$> readTextData s
      where
        infixl 3 <!>
        Left _ <!> y = y
        x      <!> _ = x

instance PersistCore ProjectM36Backend where
  newtype BackendKey ProjectM36Backend = ProjectM36Key { unPM36Key :: U.UUID }
          deriving (Show, Read, Eq, Ord, Generic, Binary, NFData, C.Atomable)

instance PersistStoreWrite ProjectM36Backend where

  insert record = do
   keys <- insertMany [record]
   return $ head keys

  --insertKey :: (Trans.MonadIO m, backend ~ PersistEntityBackend val, PersistEntity val) => Key val -> val -> ReaderT backend m ()
  insertKey key record = do
    let uuid = keyToUUID key
    _ <- insertNewRecords [uuid] [record]
    return ()

  repsert = repsertByKey
    --delete followed by insert "set the record as the truth

  replace = replaceByKey

  delete key = void (deleteByKey key)

  --update :: (MonadIO m, PersistEntity val, backend ~ PersistEntityBackend val) => Key val -> [Update val] -> ReaderT backend m ()
  update = updateByKey

  insertMany [] = return []
  insertMany records = do
    freshUUIDs <- Trans.liftIO $ mapM (const nextRandom) records
    insertNewRecords freshUUIDs records

  --insertEntityMany [] = return []

instance PersistStoreRead ProjectM36Backend where

  --get :: (MonadIO m, backend ~ PersistEntityBackend val, PersistEntity val) => Key val -> ReaderT backend m (Maybe val)
  get key = do
    ent <- lookupByKey key
    case ent of
      Nothing -> return Nothing
      Just (Entity _ val) -> return $ Just val




instance FromJSON (BackendKey ProjectM36Backend) where
  parseJSON = withText "ProjectM36Key" $ \t -> maybe (fail "Invalid UUID") (return . ProjectM36Key) (U.fromString (T.unpack t))

instance ToJSON (BackendKey ProjectM36Backend) where
  toJSON (ProjectM36Key uuid) = toJSON $ U.toString uuid

--wrapped version which throws exceptions
executeDatabaseContextExpr :: C.SessionId -> C.Connection -> DatabaseContextExpr -> IO ()
executeDatabaseContextExpr sessionId conn expr = do
  res <- C.executeDatabaseContextExpr sessionId conn expr
  case res of
    Right _ -> return ()
    Left err -> throwIO (PersistError $ T.pack (show err))

relVarNameFromRecord :: (PersistEntity record)
               => record -> RelVarName
relVarNameFromRecord = unDBName . entityDB . entityDef . Just

{- unfortunately copy-pasted from Database.Persist.Sql.Orphan.PersistStore -}
updateFieldDef :: PersistEntity v => DP.Update v -> FieldDef
updateFieldDef (DP.Update f _ _) = persistFieldDef f
updateFieldDef BackendUpdate{} = error "updateFieldDef did not expect BackendUpdate"

updatePersistValue :: DP.Update v -> PersistValue
updatePersistValue (DP.Update _ v _) = toPersistValue v
updatePersistValue BackendUpdate{} = error "updatePersistValue did not expect BackendUpdate"


instance PersistField U.UUID where
  toPersistValue val = PersistText $ T.pack (U.toString val)
  fromPersistValue (PersistText uuidText) = case U.fromString $ T.unpack uuidText of
    Nothing -> Left "uuid text read failure"
    Just uuid -> Right uuid
  fromPersistValue _ = Left $ T.pack "expected PersistObjectId"


instance PersistField (BackendKey ProjectM36Backend) where
  toPersistValue val = toPersistValue $ unPM36Key val
  fromPersistValue (PersistText uuidText) = case U.fromString $ T.unpack uuidText of
    Nothing -> Left "uuid text read failure"
    Just uuid -> Right (ProjectM36Key uuid)
  fromPersistValue _ = Left $ T.pack "expected PersistObjectId"




instance FromJSON C.ConnectionInfo where
         parseJSON v = modifyFailure ("Persistent: error loading ProjectM36 conf: " ++) $
           flip (withObject "ProjectM36Conf") v $ \o -> parseJSON =<< ( o .: "connectionType")
           

instance PersistConfig C.ConnectionInfo where
  --- | Hold onto a connection as well a default session.
         type PersistConfigBackend C.ConnectionInfo = ReaderT ProjectM36Backend
         type PersistConfigPool C.ConnectionInfo = ProjectM36Backend

         loadConfig = parseJSON
         applyEnv = return -- no environment variables are used
         createPoolConfig conf = do
           connErr <- C.connectProjectM36 conf
           case connErr of
               Left err -> throwIO $ PersistError ("Failed to create connection: " `T.append` T.pack (show err))
               Right conn -> do
                 eSession <- C.createSessionAtHead conn "master"
                 case eSession of
                   Left err -> throwIO $ PersistError ("Failed to create session: " `T.append` T.pack (show err))
                   Right sessionId -> pure (sessionId, conn)
         --runPool :: (MonadBaseControl IO m, MonadIO m) => c -> PersistConfigBackend c m a -> PersistConfigPool c -> m a
         runPool _ = runReaderT

withProjectM36Conn :: (Trans.MonadIO m) => C.ConnectionInfo -> (ProjectM36Backend -> m a) -> m a
withProjectM36Conn conf connReader = do
    backend <- Trans.liftIO $ createPoolConfig conf
    connReader backend

runProjectM36Conn :: ReaderT ProjectM36Backend m a -> (C.SessionId, C.Connection) -> m a
runProjectM36Conn m1 (sessionId, conn) = runReaderT m1 (sessionId, conn)

instance PathPiece (BackendKey ProjectM36Backend) where
    toPathPiece (ProjectM36Key uuid) = U.toText uuid
    fromPathPiece txt = do
      uuid <- U.fromText txt
      return $ ProjectM36Key uuid

instance Sql.PersistFieldSql U.UUID where
    sqlType _ = Sql.SqlOther "doesn't make much sense for ProjectM36"

instance Sql.PersistFieldSql (BackendKey ProjectM36Backend) where
    sqlType _ = Sql.SqlOther "doesn't make much sense for ProjectM36"

persistUniqueToRestrictionPredicate :: PersistEntity record => Unique record -> Either RelationalError RestrictionPredicateExpr
persistUniqueToRestrictionPredicate unique = do
    atoms <- mapM (maybe (Left $ AtomTypeNotSupported "") Right . persistValueAtom) $ persistUniqueToValues unique
    let attrNames = map (unDBName . snd) $ persistUniqueToFieldNames unique
        andify [] = TruePredicate
        andify ((attrName, atom):xs) = AndPredicate (AttributeEqualityPredicate attrName (NakedAtomExpr atom)) $ andify xs
    return $ andify (zip attrNames atoms)

uniqueBackendSetup :: PersistEntity r =>
                      Unique r -> (Either RelationalError RestrictionPredicateExpr,
                                   T.Text,
                                   EntityDef)
uniqueBackendSetup unique = (persistUniqueToRestrictionPredicate unique,
                             unDBName $ entityDB entDef,
                             entDef)
  where
    entDef = entityDef $ dummyFromUnique unique
  
instance PersistUniqueRead ProjectM36Backend where
    getBy unique = do
        (sessionId, conn) <- ask
        let (predicate, relVarName, entDef) = uniqueBackendSetup unique
            throwRelErr err = Trans.liftIO $ throwIO (PersistError (T.pack $ show err))
        case predicate of
            Left err -> throwRelErr err
            Right predicate' -> do
                let restrictExpr = Restrict predicate' (RelationVariable relVarName ())
                singletonRel <- Trans.liftIO $ C.executeRelationalExpr sessionId conn restrictExpr
                case singletonRel of
                    Left err -> throwRelErr err
                    Right singletonRel'
                      | cardinality singletonRel' == Finite 0 ->
                            return Nothing
                      | cardinality singletonRel' > Finite 1 ->
                          Trans.liftIO $ throwIO (PersistError "getBy returned more than one tuple")
                      | otherwise ->
                          case singletonTuple singletonRel' of
                            Nothing -> Trans.liftIO $ throwIO (PersistMarshalError "singletonTuple failure")
                            Just tuple -> do
                              newEnt <- Trans.liftIO $ fromPersistValuesThrow entDef tuple
                              return $ Just newEnt

instance PersistUniqueWrite ProjectM36Backend where
    deleteBy unique = do
        (sessionId, conn) <- ask
        let (predicate, relVarName, _) = uniqueBackendSetup unique        
            throwRelErr err = Trans.liftIO $ throwIO (PersistError (T.pack $ show err))
        case predicate of
            Left err -> throwRelErr err
            Right predicate' -> do
                let deleteExpr = Delete relVarName predicate'
                eiRes <- Trans.liftIO $ C.executeDatabaseContextExpr sessionId conn deleteExpr
                case eiRes of
                   Left err -> throwRelErr err
                   Right _ -> return ()

multiFilterAsRestrictionPredicate :: (PersistEntity val, PersistEntityBackend val ~ ProjectM36Backend) => Bool -> [Filter val] -> Either RelationalError RestrictionPredicateExpr
multiFilterAsRestrictionPredicate _ [] = Right TruePredicate
multiFilterAsRestrictionPredicate andOr (x:xs) = do
    let predicate = if andOr then AndPredicate else OrPredicate
    filtHead <- filterAsRestrictionPredicate x
    filtTail <- multiFilterAsRestrictionPredicate andOr xs
    return $ predicate filtHead filtTail

filterValueToPersistValues :: forall a. PersistField a => Either a [a] -> [PersistValue]
filterValueToPersistValues v = map toPersistValue $ either return id v

filterAsRestrictionPredicate :: (PersistEntity val, PersistEntityBackend val ~ ProjectM36Backend) => Filter val -> Either RelationalError RestrictionPredicateExpr
filterAsRestrictionPredicate filterIn = case filterIn of
    FilterAnd filters -> multiFilterAsRestrictionPredicate True filters
    FilterOr filters -> multiFilterAsRestrictionPredicate False filters
    BackendFilter _ -> error "BackendFilter not supported"
    Filter field value pfilter -> let attrName = unDBName $ fieldDB (persistFieldDef field) in
                                  case value of
                                      Left val -> let atom = persistValueAtom $ toPersistValue val in
                                        case pfilter of
                                          Eq -> case atom of
                                                    Nothing -> Left $ AtomTypeNotSupported attrName
                                                    Just atom' -> Right $ AttributeEqualityPredicate attrName (NakedAtomExpr atom')
                                          Ne -> case atom of
                                                    Nothing -> Left $ AtomTypeNotSupported attrName
                                                    Just atom' -> Right $ NotPredicate (AttributeEqualityPredicate attrName (NakedAtomExpr atom'))
                                          op -> Left $ AtomOperatorNotSupported $ T.pack (show op)
                                      Right _ -> Left $ AtomTypeNotSupported attrName

updateToUpdateTuple :: (PersistEntity val, PersistEntityBackend val ~ ProjectM36Backend) => DPT.Update val -> Either RelationalError (AttributeName, AtomExpr)
updateToUpdateTuple (BackendUpdate _) = error "BackendUpdate not supported"
updateToUpdateTuple (DPT.Update field value op) = let attrName = unDBName $ fieldDB (persistFieldDef field)
                                                      atom = persistValueAtom $ toPersistValue value
                                                  in case op of
                                                       DPT.Assign -> case atom of
                                                         Nothing -> Left $ AtomTypeNotSupported attrName
                                                         Just atom' -> Right (attrName, NakedAtomExpr atom')
                                                       op' -> Left $ AtomOperatorNotSupported $ T.pack (show op')

selectionFromRestriction :: (PersistEntity val, PersistEntityBackend val ~ backend, Trans.MonadIO m, backend ~ ProjectM36Backend) => [Filter val] -> ReaderT backend m (Either RelationalError RelationalExpr)
selectionFromRestriction filters = 
    runExceptT $ do
        restrictionPredicate <- hoistEither $ multiFilterAsRestrictionPredicate True filters
        let entDef = entityDef $ dummyFromFilters filters
            relVarName = unDBName $ entityDB entDef
        right $ Restrict restrictionPredicate (RelationVariable relVarName ())


instance PersistQueryWrite ProjectM36Backend where
         updateWhere _ [] = return ()
         updateWhere filters updates = do
             (sessionId, conn) <- ask
             e <- runExceptT $ do
                 restrictionPredicate <- hoistEither $ multiFilterAsRestrictionPredicate True filters
                 tuples <- hoistEither $ mapM updateToUpdateTuple updates
                 let updateMap = M.fromList tuples
                     entDef = entityDef $ dummyFromUpdate (head updates)
                     relVarName = unDBName $ entityDB entDef
                     updateExpr = Update relVarName updateMap restrictionPredicate
                 eiRes <- Trans.liftIO $ C.executeDatabaseContextExpr sessionId conn updateExpr
                 case eiRes of
                     Left err -> left err
                     Right _ -> right ()
             case e of
               Left err -> throwIOPersistError ("updateWhere failure: " ++ show err)
               Right () -> return ()

         deleteWhere filters = do
             (sessionId, conn) <- ask
             e <- runExceptT $ do
                 restrictionPredicate <- hoistEither $ multiFilterAsRestrictionPredicate True filters
                 let entDef = entityDef $ dummyFromFilters filters
                     relVarName = unDBName $ entityDB entDef
                     deleteExpr = Delete relVarName restrictionPredicate
                 eiRes <- Trans.liftIO $ C.executeDatabaseContextExpr sessionId conn deleteExpr
                 case eiRes of
                     Left err -> left err
                     Right _  -> right ()
             case e of
               Left err -> throwIOPersistError ("deleteWhere failure: " ++ show err)
               Right () -> return ()


instance PersistQueryRead ProjectM36Backend where
         count filters = do
             (sessionId, conn) <- ask
             e <- runExceptT $ do
                 restrictionPredicate <- hoistEither $ multiFilterAsRestrictionPredicate True filters
                 let entDef = entityDef $ dummyFromFilters filters
                     relVarName = unDBName $ entityDB entDef
                     allAttrNamesList = map (unDBName . fieldDB) (entityFields entDef) ++ [unDBName (fieldDB (entityId entDef))]
                     allAttrNames = AttributeNames $ S.fromList allAttrNamesList
                     groupExpr = Group allAttrNames "persistcountrel" (Restrict restrictionPredicate (RelationVariable relVarName ()))
                     tupleExpr = AttributeExtendTupleExpr "persistcount" (FunctionAtomExpr "count" [AttributeAtomExpr "persistcountrel"] ())
                     countExpr = Extend tupleExpr groupExpr
                 rel <- Trans.liftIO $ C.executeRelationalExpr sessionId conn countExpr
                 case rel of
                    Left err -> left err
                    Right rel' -> case singletonTuple rel' of
                          Nothing -> Trans.liftIO $ throwIO $ PersistError "failed to get count tuple"
                          Just tuple -> case atomForAttributeName "persistcount" tuple of
                             (Right c) -> return (castInt c)
                             Left err -> left err
             case e of
               Left err -> throwIOPersistError ("count failure: " ++ show err)
               Right c -> return c

         selectSourceRes filters limitOffset = do
             (sessionId, conn) <- ask
             entities <- runExceptT $ do
                 restrictionExpr <- lift (selectionFromRestriction filters) >>= hoistEither
                 --restrictionExpr' <- hoistEither restrictionExpr
                 let entDef = entityDef $ dummyFromFilters filters
                     tupleMapper tuple = Trans.liftIO $ fromPersistValuesThrow entDef tuple
                     (orderExprs', mLimit', mOffset') = processSelectOpts limitOffset
                     dataFrameExpr = DF.DataFrameExpr {
                       DF.convertExpr = restrictionExpr,
                       DF.orderExprs = orderExprs',
                       DF.offset = mOffset',
                       DF.limit = mLimit'
                       }
                 eDf <- Trans.liftIO $ C.executeDataFrameExpr sessionId conn dataFrameExpr
                 case eDf of
                     Left err -> Trans.liftIO $ throwIO $ PersistError (T.pack (show err))
                     Right df@DF.DataFrame{} -> mapM tupleMapper (DF.tuples df)
             case entities of
                 Left err -> Trans.liftIO $ throwIO $ PersistError (T.pack $ show err)
                 Right entities' -> return $ return $ CL.sourceList entities'

         selectKeysRes _ (_:_) = Trans.liftIO $ throwIO $ PersistError "select options not yet supported"
         selectKeysRes filters [] = do
            (sessionId, conn) <- ask
            keys <- runExceptT $ do
               restrictionExpr <- lift (selectionFromRestriction filters) >>= hoistEither
               let keyAttrNames = ["id"] --no support for multi-attribute keys yet
                   keyExpr = Project (AttributeNames $ S.fromList keyAttrNames) restrictionExpr
               (Relation _ tupleSet) <- Trans.liftIO (C.executeRelationalExpr sessionId conn keyExpr) >>= hoistEither
               let keyMapper :: (PersistEntity record, Trans.MonadIO m) => RelationTuple -> ExceptT RelationalError (ReaderT ProjectM36Backend m) (Key record)
                   keyMapper tuple = do
                                       atoms <- hoistEither (atomsForAttributeNames (V.fromList keyAttrNames) tuple)
                                       case keyFromValues (map atomAsPersistValue (V.toList atoms)) of
                                           Left err -> throwIOPersistError ("keyFromValues failure: " ++ show err)
                                           Right key -> right key
               mapM keyMapper $ asList tupleSet
            case keys of
                Left err -> throwIOPersistError ("keyFromValues failure2: " ++ show err)
                Right keys' -> return $ return (CL.sourceList keys')

right :: (Trans.MonadIO m) => a -> m a
right = Trans.liftIO . return

left :: Monad m => e -> ExceptT e m a
left = throwE

--deriving instance Generic U.UUID
instance C.Atomable U.UUID where
  toAtom = TextAtom . U.toText
  fromAtom (TextAtom t) = fromJust $ U.fromText t  -- dangerous conversion
  fromAtom _ = error "improper fromAtom"  
  toAtomType _ = TextAtomType
  toAddTypeExpr _ = NoOperation

entityInfo :: PersistEntity r => r -> [(EmbedFieldDef, PersistValue)]
entityInfo record = zip entFields entValues
  where
    entFields = embeddedFields $ toEmbedEntityDef (entityDef $ Just record)
    entValues = map toPersistValue $ toPersistFields record

toDefineExprWithId ::forall record. PersistEntity record => record -> RelVarName -> Either RelationalError DatabaseContextExpr
toDefineExprWithId record rvName = do
{-  let entInfo =
  let entInfo = zip entFields entValues
      entFields = embeddedFields $ toEmbedEntityDef (entityDef $ Just record)
      entValues = map toPersistValue $ toPersistFields record-}
  attrVec <- recordAttributes (entityInfo record)  record
  return $ Define rvName (map NakedAttributeExpr (V.toList attrVec))

processSelectOpts :: PersistEntity record => [SelectOpt record] -> ([C.AttributeOrderExpr], Maybe Integer, Maybe Integer)
processSelectOpts options = (orderings, mLimit, mOffset)
  where
    attrName record = unDBName $ fieldDB (persistFieldDef record)
    orderings = foldr processAttributeOrdering [] options
    processAttributeOrdering opt accum = case opt of
      Asc attr -> DF.AttributeOrderExpr (attrName attr) DF.AscendingOrder : accum
      Desc attr -> DF.AttributeOrderExpr (attrName attr) DF.DescendingOrder : accum
      OffsetBy _ -> accum
      LimitTo _ -> accum
    mLimit = foldr (\opt accum -> case opt of
                       Asc _ -> accum
                       Desc _ -> accum
                       OffsetBy _ -> accum
                       LimitTo val -> Just (fromIntegral val)) Nothing options
    mOffset = foldr (\opt accum -> case opt of
                        Asc _ -> accum
                        Desc _ -> accum
                        OffsetBy c -> Just (fromIntegral c)
                        LimitTo _ -> accum) Nothing options
    
      
