package surfstore;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.*;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import io.grpc.stub.StreamObserver;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import surfstore.SurfStoreBasic.*;


public final class MetadataStore {
    private static final Logger logger = Logger.getLogger(MetadataStore.class.getName());

    protected Server server;
	protected ConfigReader config;

    public MetadataStore(ConfigReader config) {
    	this.config = config;
	}

	private void start(int port, int numThreads) throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(new MetadataStoreImpl(config))
                .executor(Executors.newFixedThreadPool(numThreads))
                .build()
                .start();
        logger.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                MetadataStore.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    private static Namespace parseArgs(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("MetadataStore").build()
                .description("MetadataStore server for SurfStore");
        parser.addArgument("config_file").type(String.class)
                .help("Path to configuration file");
        parser.addArgument("-n", "--number").type(Integer.class).setDefault(1)
                .help("Set which number this server is");
        parser.addArgument("-t", "--threads").type(Integer.class).setDefault(10)
                .help("Maximum number of concurrent threads");

        Namespace res = null;
        try {
            res = parser.parseArgs(args);
        } catch (ArgumentParserException e){
            parser.handleError(e);
        }
        return res;
    }

    public static void main(String[] args) throws Exception {
        Namespace c_args = parseArgs(args);
        if (c_args == null){
            throw new RuntimeException("Argument parsing failed");
        }
        
        File configf = new File(c_args.getString("config_file"));
        ConfigReader config = new ConfigReader(configf);

        if (c_args.getInt("number") > config.getNumMetadataServers()) {
            throw new RuntimeException(String.format("metadata%d not in config file", c_args.getInt("number")));
        }

        final MetadataStore server = new MetadataStore(config);
        server.start(config.getMetadataPort(c_args.getInt("number")), c_args.getInt("threads"));
        server.blockUntilShutdown();
    }

    static class MetadataStoreImpl extends MetadataStoreGrpc.MetadataStoreImplBase {
        
        protected HashMap<String, FileInfo> mdataMap; 
        private final ManagedChannel blockChannel;
        private final BlockStoreGrpc.BlockStoreBlockingStub blockStub;

        public MetadataStoreImpl(ConfigReader config)
        {
            super();
            this.mdataMap = new HashMap<String, FileInfo>();    
            this.blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort())
                                .usePlaintext(true).build();
            this.blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);
        }

    
        public void shutdown() throws InterruptedException {
            blockChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }
        
        @Override
        public void ping(Empty req, final StreamObserver<Empty> responseObserver) {
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
        /**
         *
         * Read the requested file.
         * (v, hl) = ReadFile(FileInfo with filename filled in)  
         * The client only needs to supply the "filename" argument of FileInfo.
         * The server only needs to fill the "version" and "blocklist" fields.
         * If the file does not exist, "version" should be set to 0.
         * This command should return an error if it is called on a server
         * that is not the leader
         */
        @Override
        public void readFile(surfstore.SurfStoreBasic.FileInfo request,
            io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.FileInfo> responseObserver) {
          
            // Get hash entry pointed to by request.getFilename()
            FileInfo response; 
            String filename = request.getFilename();
            logger.info("Getting metadata for file: " + filename); 
            
            // TODO: if server is not leader, issue error 
            
            //If it exists, return fileinfo with version # and hashlist
            if (mdataMap.containsKey(filename)) {
                FileInfo info = mdataMap.get(filename);
                if (info.getBlocklistList().get(0).equals("0")) {
                   // file has been deleted & we're just keeping it around 
                   // return with v = current_version and hl = ["0"].
                    response = myUtils.createFileInfo(filename, info.getVersion(), info.getBlocklistList());    
                } else {
                    response = info; 
                }
            } 
            // File does not exist in metdata store, return Fileinfo with v=0. 
            else {
                response = myUtils.createFileInfo(filename, 0, null);     
            }
            responseObserver.onNext(response);
            responseObserver.onCompleted();  
            return; 
        }
    
        /**
         * Write a file.
         * The client must specify all fields of the FileInfo message.
         * The server returns the result of the operation in the "result" field.
         *
         * The server ALWAYS sets "current_version", regardless of whether
         * the command was successful. If the write succeeded, it will be the
         * version number provided by the client. Otherwise, it is set to the
         * version number in the MetadataStore.
         *
         * If the result is MISSING_BLOCKS, "missing_blocks" contains a
         * list of blocks that are not present in the BlockStore.
         *
         * This command should return an error if it is called on a server
         * that is not the leader
         */
        @Override
        public void modifyFile(surfstore.SurfStoreBasic.FileInfo request,
            io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.WriteResult> responseObserver) {
            // TODO: if client doesn't specify all fields of FileInfo, issue error 
            
            // two phase commit stuff
            boolean successFlag = false;
            FileInfo newf = null;
            
            String filename = request.getFilename(); 
            int version = request.getVersion(); 
            List<String> hl = request.getBlocklistList();

            logger.info("Adding/modifying file with name: " + filename); 
           
            //Generate response
            WriteResult.Builder wr = WriteResult.newBuilder();


            //Check Blockstore to ensure that all modified blocks in the hashlist exist;
            List<String> missing = new ArrayList<String>();
            boolean has_missing = false; 
            for (int i=0; i < hl.size(); i=i+1) { 
                Block check = myUtils.hashToBlock(hl.get(i));
                if (blockStub.hasBlock(check).getAnswer() == false) {
                    missing.add(hl.get(i));
                    has_missing = true; 
                }
            }         

            if (has_missing == false) {
                // if file exists, update its hl to have hl in request and increase
                // version number to request.v. Ensure that request.v = v + 1. 
                if (mdataMap.containsKey(filename) == true) {
                    FileInfo info = mdataMap.get(filename);
                    if (version != info.getVersion() + 1) {
                            logger.info("ERROR! Version # is wrong");
                            wr.setResult(WriteResult.Result.OLD_VERSION);
                            wr.setCurrentVersion(info.getVersion());
                    } 
                    else {
                        newf = myUtils.createFileInfo(filename, version, hl);
                        logger.info("File exists (or has been deleted)"); 
                        if (info.getBlocklistList().get(0).equals("0")) {
                            //File has been deleted and will now be readded
                            // I don't know if this has to be handled separately... 
                            //mdataMap.put(filename, newf);
                            successFlag = true;
                            wr.setResult(WriteResult.Result.OK);
                            wr.setCurrentVersion(version);
                        }
                        // File already exists, just update version + hl
                        else {
                            //mdataMap.put(filename, newf);
                            successFlag = true;
                            wr.setResult(WriteResult.Result.OK);
                            wr.setCurrentVersion(version); 
                        }
                    }
                }
                // New file! Version should be 1. 
                else {
                    if (version == 1) {
                        logger.info("Adding new file"); 
                        FileInfo new_file = myUtils.createFileInfo(filename, version, hl);
                        //mdataMap.put(filename, new_file);
                        successFlag = true;
                        wr.setResult(WriteResult.Result.OK);
                        wr.setCurrentVersion(version);
                    } else {
                        logger.info("ERROR! Version # is wrong");
                        wr.setResult(WriteResult.Result.OLD_VERSION);
                        wr.setCurrentVersion(version);
                    }
                } 
            } else {
               // This hashlist is not fully in the blockstore. 
                logger.info("Missing blocks!");
                wr.setResult(WriteResult.Result.MISSING_BLOCKS);
                wr.setCurrentVersion(version);
                wr.addAllMissingBlocks(missing); 
            } 
        
            WriteResult response = wr.build();
            response = twoPhaseCommit(successFlag, filename, newf, response);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            return;
        }
   
        /**
         * <pre>
         * Delete a file.
         * This has the same semantics as ModifyFile, except that both the
         * client and server will not specify a blocklist or missing blocks.
         * As in ModifyFile, this call should return an error if the server
         * it is called on isn't the leader
         * </pre>
         */
        @Override
        public void deleteFile(surfstore.SurfStoreBasic.FileInfo request,
            io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.WriteResult> responseObserver) {
 
            // TODO: if client doesn't specify all fields of FileInfo, issue error 
            
            // two phase commit stuff
            boolean successFlag = false;
            FileInfo newf = null;
            
            String filename = request.getFilename(); 
            int version = request.getVersion(); 

            logger.info("Deleting file with name: " + filename); 
           
            //Generate response
            WriteResult.Builder wr = WriteResult.newBuilder(); 
            List<String> empty_hl = new ArrayList<String>();
            empty_hl.add("0");

            if (mdataMap.containsKey(filename) == true) {
                FileInfo info = mdataMap.get(filename);
                if (version != info.getVersion()+1) {
                    logger.info("Version is wrong");
                    wr.setResult(WriteResult.Result.OLD_VERSION);
                    wr.setCurrentVersion(info.getVersion());
                } else {
                    newf = myUtils.createFileInfo(filename, version, empty_hl);
                    //mdataMap.remove(filename);
                    //mdataMap.put(filename, newf);
                    successFlag = true;
                    wr.setResult(WriteResult.Result.OK);
                    wr.setCurrentVersion(version);
                }
            } 
            //Trying to delete file that's not in the store? what happens here? 
            else {
                logger.info("Trying to delete file not on server");
		wr.setResult(WriteResult.Result.MISSING_BLOCKS);
            }

            WriteResult response = wr.build();
            response = twoPhaseCommit(successFlag, filename, newf, response);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            return;
        }
    
        /**
         * <pre>
         * Query whether the MetadataStore server is currently the leader.
         * This call should work even when the server is in a "crashed" state
         * </pre>
         */
        @Override
        public void isLeader(surfstore.SurfStoreBasic.Empty request,
            io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.SimpleAnswer> responseObserver) {
            return; 
        }
    
        /**
         * <pre>
         * "Crash" the MetadataStore server.
         * Until Restore() is called, the server should reply to all RPCs
         * with an error (except Restore) and not send any RPCs to other servers.
         * </pre>
         */
        @Override
        public void crash(surfstore.SurfStoreBasic.Empty request,
            io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.Empty> responseObserver) {
            return; 
        }
    
        /**
         * <pre>
         * "Restore" the MetadataStore server, allowing it to start
         * sending and responding to all RPCs once again.
         * </pre>
         */
        @Override
        public void restore(surfstore.SurfStoreBasic.Empty request,
            io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.Empty> responseObserver) {
            return;
        }
    
        /**
         * <pre>
         * Find out if the node is crashed or not
         * (should always work, even if the node is crashed)
         * </pre>
         */
        @Override
        public void isCrashed(surfstore.SurfStoreBasic.Empty request,
            io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.SimpleAnswer> responseObserver) {
            return;
        }
        
        // Run two-phase commit on an operation
        private WriteResult twoPhaseCommit(boolean successFlag, String mapKey, FileInfo mapValue, WriteResult origResult) {
            if (!successFlag) {
                return origResult;
            }
            // do majority follower check here
            if (true) {
                mdataMap.put(mapKey, mapValue);
                return origResult;
            } else {
                // construct error about majority follower check not passing here and return it
            }
        }
    }
}
