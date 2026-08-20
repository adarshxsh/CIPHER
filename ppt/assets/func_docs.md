1. Publisher Module
Content Preparation
func ChunkFile(file, chunkConfig) – < 
func EncodeReedSolomon(chunks, rsConfig) 
func BuildManifest(shards, metadata)
func GenerateMerkleRoot(shards)


Manifest struct : file ID,
chunk size,
number of chunks,
DataShards,
ParityShards
Merkle root,
hash of each chunk - ChunkID

Provider Discovery
func RequestProviders(providerRequirement) 

Returns
ProviderList {json } 


Placement
func AssignChunksToProviders(manifest, providerList)

Returns
PlacementMap


Publication
func PublishManifest(manifest)

Returns 
ManifestID

func PublishPlacementMap(manifestID, placementMap)

Returns
PlacementMapID


Distribution – ** 
func UploadAssignedShards(placementMap)


2. Control Plane (Kademlia)
Provider Registry
func RegisterProvider(providerMetadata)

func UpdateProviderState(providerID, heartbeat)

func QueryAvailableProviders(filter)

func RemoveProvider(providerID)


Manifest Registry
func RegisterManifest(manifest)

func ResolveManifest(manifestID)


Placement Registry
func RegisterPlacementMap(manifestID, placementMap)

func ResolvePlacementMap(manifestID)


Discovery
func FindProviders(contentID)

func FindShardProviders(shardID)



– Kademlia Architecute 
	Enums
		Struct  
	API – calls response 
	

	
	
3. Provider Module
Lifecycle
func Register(metadata)

func Heartbeat(providerState)


Storage
func StoreShard(shard)

func ReadShard(shardID)


Serving
func HandleShardRequest(request)


4. Client Module
Bootstrap
func ResolveManifest(manifestID)

func ResolvePlacementMap(manifestID)


Planning
func BuildChunkProviderMap(placementMap)

Returns
Chunk → Provider


Download
func StartParallelDownload(chunkProviderMap)

func RequestShard(providerID, shardID)

func VerifyShard(shard, proof)

func StoreShard(shard)


Recovery
func ResumeDownload(sessionID)

func SwitchProvider(shardID, providerID)


5. Network (Transport)
func ConnectProvider(providerID)

func EstablishDirectConnection(providerID)

func OpenChunkStream(providerID)

func SendProtocolMessage(streamID, message)

func ReceiveProtocolMessage(streamID)

func CloseChunkStream(streamID)


Naming Dictionary
To keep everything consistent across the project:
Architecture Term
Recommended Name
Manifest
Manifest
Manifest ID
ManifestID
Chunk → Provider mapping
PlacementMap
Placement identifier
PlacementMapID
Provider list
ProviderList
Chunk assignment
AssignChunksToProviders()
Client execution plan
ChunkProviderMap
Parallel download
StartParallelDownload()
Provider heartbeat
UpdateProviderState()
Provider metadata
ProviderMetadata
Provider state
ProviderState
Shard lookup
FindShardProviders()

