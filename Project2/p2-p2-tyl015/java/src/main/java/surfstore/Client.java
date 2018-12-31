package surfstore;

import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import java.util.*;
import com.google.protobuf.ByteString;
import surfstore.SurfStoreBasic.*;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;


public final class Client {
    private static final Logger logger = Logger.getLogger(Client.class.getName());

    private final ManagedChannel metadataChannelLeader;
    private final MetadataStoreGrpc.MetadataStoreBlockingStub metadataStubLeader;

    private final ArrayList<ManagedChannel> followerChannels;
    private final ArrayList<MetadataStoreGrpc.MetadataStoreBlockingStub> followerStubs; 

    private final ManagedChannel blockChannel;
    private final BlockStoreGrpc.BlockStoreBlockingStub blockStub;

    private final ConfigReader config;
    private final int leader;

    public Client(ConfigReader config) {

        this.followerChannels = new ArrayList<ManagedChannel>();
        this.followerStubs = new ArrayList<MetadataStoreGrpc.MetadataStoreBlockingStub>(); 
        this.leader = config.getLeaderNum();

        for (int i = 0; i < config.getNumMetadataServers(); i++) {
            ManagedChannel channel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(i))
                                                        .usePlaintext(true).build();
            this.followerChannels.add(channel); 
            this.followerStubs.add(MetadataStoreGrpc.newBlockingStub(channel));
        }
        this.blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort())
                .usePlaintext(true).build();
        this.blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);

        this.config = config;
    }

    public void shutdown() throws InterruptedException {
        metadataChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        blockChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }
    
    //If bool is false, crash program
    private void ensure(boolean b) {
        if (b == false) {
            throw new RuntimeException("Assertion failed!"); 
        }
    }


    private void testBlockStore() {
        String h1 = myUtils.genHash("block_01");
        String h2 = myUtils.genHash("block_02");

        Block b1 = myUtils.hashToBlock(h1); 
        Block b2 = myUtils.hashToBlock(h2); 
        
        
        //has block wil tell you true/false if it exists in the store
        ensure(blockStub.hasBlock(b1).getAnswer() == false); //should return false
        ensure(blockStub.hasBlock(b2).getAnswer() == false); //should return false

        blockStub.storeBlock(b1);
        ensure(blockStub.hasBlock(b1).getAnswer() == true);
        ensure(blockStub.hasBlock(b2).getAnswer() == false);
    
        blockStub.storeBlock(b2);
        ensure(blockStub.hasBlock(b2).getAnswer() == true);

        // Get a block, will return 
        Block b1prime = blockStub.getBlock(b1); 

        //b1 should be equal to b1prime
        ensure(b1prime.getHash().equals(b1.getHash()));
        ensure(b1.getHash().equals(b1prime.getHash()));

    }

    private void testMetadataStore() {

        String h1 = myUtils.genHash("block_01"); 
        String h2 = myUtils.genHash("block_02");
        String h3 = myUtils.genHash("block_03");
        String h4 = myUtils.genHash("block_04");

        Block b1 = myUtils.hashToBlock(h1); 
        Block b2 = myUtils.hashToBlock(h2); 
        Block b3_not = myUtils.hashToBlock(h3); 
        Block b4_not = myUtils.hashToBlock(h4);
 
        blockStub.storeBlock(b1);
        blockStub.storeBlock(b2);

        // Sample files for tests
        List<String> hl = new ArrayList<String>(); 
        hl.add(h1);
        hl.add(h2);

        //Test 1: file not in blockstore
        FileInfo f1_out = metadataStub.readFile(myUtils.stringToFileInfo("myfile1.txt"));
        ensure(f1_out.getVersion() == 0);
        //Should hl be null here or empty? 
        

        // Test 2: Assume blocks h1/h2 are in the BlockStore, but we call
        // the metadata store with the wrong version (2). This should
        // return OLD_VERSION as error, and readfile will be wrong. 
        FileInfo f1 = myUtils.createFileInfo("myfile1.txt", 2, hl); 
        WriteResult res1 = metadataStub.modifyFile(f1);
        // Provided wrong version, check that res1.Result = OLD_VERSION 
        ensure(res1.getResult() == WriteResult.Result.OLD_VERSION);

        FileInfo f1_read = metadataStub.readFile(myUtils.stringToFileInfo("myfile1.txt"));
        //Check v = 0 since provided v of 2 instead of correct value of 1
        ensure(f1_read.getVersion() == 0);
 
        // Test 3: Now assume version is correct. This should be 
        // correct, and return OK for result. Read file will give correct
        // version of 1.  
        FileInfo f2 = myUtils.createFileInfo("myfile1.txt", 1, hl); 
        WriteResult res2 = metadataStub.modifyFile(f2);
        // check that res1.Result = OK 
        ensure(res2.getResult() == WriteResult.Result.OK);

        FileInfo f2_read = metadataStub.readFile(myUtils.stringToFileInfo("myfile1.txt"));
        List<String> f2_ret_hl = f2_read.getBlocklistList();

        //Check v = 1 and return hashlist, it should equal hl.
        ensure(f2_read.getVersion() == 1);
        ensure(f2_ret_hl.equals(hl));

        // Test 4: Assume 2 blocks (in hl) are part of myfile1.txt
        // but they are not in blockstore, so htis should return
        // missing blocks.
        // Check that this results in an error, with MISSING_BLOCKS. 
        hl.add(h3);
        hl.add(h4);
        FileInfo f3 = myUtils.createFileInfo("myfile1.txt", 2, hl); 
        WriteResult res3 = metadataStub.modifyFile(f3);
        // check that res1.Result = MISSING_BLOCK
        ensure(res3.getResult() == WriteResult.Result.MISSING_BLOCKS);
        List<String> new_hl = res3.getMissingBlocksList();
        ensure(new_hl.get(0).equals(h3));
        ensure(new_hl.get(1).equals(h4));

        
        // Test 5.1: Add some blocks, check ok. Add more blocks, check ok.  
        List<String> hl2 = new ArrayList<String>();
        hl2.add(h1); 
        hl2.add(h2); 
        FileInfo f4 = myUtils.createFileInfo("myfile2.txt", 1, hl2);
        WriteResult res4 = metadataStub.modifyFile(f4);
        ensure(res4.getResult() == WriteResult.Result.OK);
        
        //Check v = 1
        FileInfo f4_read = metadataStub.readFile(myUtils.stringToFileInfo("myfile2.txt"));
        ensure(f4_read.getVersion() == 1);
        
        //Check return hashlist equals hashlist2
        ensure(f4_read.getBlocklistList().equals(hl2));


        // Test 5.2: Push in wrong version
        FileInfo f4_err = myUtils.createFileInfo("myfile2.txt", 1, hl2);
        WriteResult res4_err = metadataStub.modifyFile(f4_err);
        // Provided wrong version, check that res1.Result = OLD_VERSION 
        ensure(res4_err.getResult() == WriteResult.Result.OLD_VERSION);


        // Test 5.3: Add more blocks, try to update.modify 
        blockStub.storeBlock(b3_not);
        blockStub.storeBlock(b4_not);
        hl2.add(h3);
        hl2.add(h4);
        FileInfo f4_mod = myUtils.createFileInfo("myfile2.txt", 2, hl2);
 
        WriteResult res4_mod = metadataStub.modifyFile(f4_mod);
        ensure(res4.getResult() == WriteResult.Result.OK);
        
        //Check v = 2
        FileInfo f4_mod_read = metadataStub.readFile(myUtils.stringToFileInfo("myfile2.txt"));
        ensure(f4_mod_read.getVersion() == 2);
        
        //Check return hashlist equals hashlist2
        ensure(f4_mod_read.getBlocklistList().equals(hl2));

        //metadataStub.DeleteFile("myfile1.txt", 2);
        //metadataStub.ReadFile("myfile2.txt");
        ////Check v = 2
        ////Check return hashlist equals hashlist2_modified, since deletion should have failed
        //
        //metadataStub.DeleteFile("myfile1.txt", 3);
        //metadataStub.ReadFile("myfile2.txt");
        ////Check v = 3
        ////Somehow check file was deleted?

    }
    
    private void testReadWriteFile() {
        ArrayList<Block> myBlocks = myUtils.fileToBlockList("test.txt");
        System.err.println("test.jpg has: " + myBlocks.size() + " Blocks");
        for (int x = 0; x < myBlocks.size(); x++) {
            Block tempblock = myBlocks.get(x);
            int temp = tempblock.getData().size();
            String tempstr = "Block " + x + " has size: " + temp;
            System.err.println(tempstr);
        }
        
        myUtils.blockListToFile(myBlocks, "test2.txt");
    }
    
	private void go() {
		metadataStub.ping(Empty.newBuilder().build());
        logger.info("Successfully pinged the Metadata server");
    
        blockStub.ping(Empty.newBuilder().build());
        logger.info("Successfully pinged the Blockstore server");

        //testBlockStore();
        //testMetadataStore();
        //testReadWriteFile();

        logger.info("Passed tests");
	}

    private void handleClient(String c, String fname, String store_dir) {
        switch (c) {
            case "upload":
                upload(fname);
		break;
            case "download":
                download(fname, store_dir);
                break;
            case "delete":
                delete(fname);
		break;
            case "getversion":
                getversion(fname);
		break; 
            default:
                logger.info("ERROR: command not found " + c);
		break;
        }
    }


    private void upload(String filename) {

        // Check if local file exists on disk; if not, issue std error and return
        if (myUtils.fileExists(filename) == false) {
            System.out.println("Not Found");
            return;
        } else { 
            // check ReadFile to see whether it exists in metadata & get version
            FileInfo fileRead = metadataStub.readFile(myUtils.stringToFileInfo(filename));
            ArrayList<Block> blocklist = myUtils.fileToBlockList(filename);     

            // new file, compute hashlist & call modifyFile with version 1
            List<String> hashlist = myUtils.blocksToHashList(blocklist);
            WriteResult wr = metadataStub.modifyFile(myUtils.createFileInfo(filename, fileRead.getVersion()+1, hashlist));
            
            // get which blocks are missing after trying to add 
            if (wr.getResult() == WriteResult.Result.MISSING_BLOCKS) {
                List<String> missing = wr.getMissingBlocksList();
                for (int i = 0; i < blocklist.size(); i++) {
                    if (missing.contains(blocklist.get(i).getHash())) {
                        blockStub.storeBlock(blocklist.get(i));
                    }
                }
                WriteResult wr_mod = metadataStub.modifyFile(myUtils.createFileInfo(filename, fileRead.getVersion()+1, hashlist));
            }
            else if (wr.getResult() == WriteResult.Result.OLD_VERSION) {
                logger.info("ERROR: Requires version >= " + wr.getCurrentVersion()+1);
            }
	    System.out.println("OK");
            return;  
        } 
    }

    private void download(String filename, String store_dir) {
        //ArrayList<String> myUtils.fileToBlockList(filename);
        
        // check file exists in store
        List<String> myHashes = metadataStub.readFile(myUtils.stringToFileInfo(filename)).getBlocklistList();
        if (myHashes.size() == 0 || myHashes.get(0).equals("0")) {
            System.out.println("Not Found");
            return;
        }
        
        // generate hashmap for contents of all files in directory
        HashMap<String, Block> blockMap = new HashMap<String, Block>();
        
        File folder = new File(store_dir);
        File[] listOfFiles = folder.listFiles();
        
        for (int i = 0; i < listOfFiles.length; i++) {
            if (listOfFiles[i].isFile()) { // for each file
                // get Blocks for inputting into hashmap
                ArrayList<Block> tempBlocks = myUtils.fileToBlockList(listOfFiles[i].getPath());
                // iterate through all Blocks and insert into hashmap
                for (Block tempBlock : tempBlocks) {
                    if (!(blockMap.containsKey(tempBlock.getHash()))) {
                        blockMap.put(tempBlock.getHash(), tempBlock);
                    }
                }
            }
        }
        
        // generate arraylist of Blocks
        ArrayList<Block> myBlocks = new ArrayList<Block>();
        
        for (String tempHash: myHashes) {
            if (blockMap.containsKey(tempHash)) {
                myBlocks.add(blockMap.get(tempHash));
            } else { // contact BlockStore for Blocks with hash not in any of the files in directory
                myBlocks.add(blockStub.getBlock(myUtils.hashToBlock(tempHash)));
            }
        }
        
        // create file path
        String myfilepath = "";
        if (store_dir.substring(store_dir.length() - 1).equals("/")) {
            myfilepath = store_dir + filename;
        } else {
            myfilepath = store_dir + "/" + filename;
        }
        
        // write to filepath
        myUtils.blockListToFile(myBlocks, myfilepath);
        System.out.println("OK"); 
        return;
    }

    private void delete(String filename) {  
        FileInfo fileRead = metadataStub.readFile(myUtils.stringToFileInfo(filename));
        WriteResult wr = metadataStub.deleteFile(myUtils.createFileInfo(filename, fileRead.getVersion()+1, null)); 
	if (wr.getResult() == WriteResult.Result.MISSING_BLOCKS) {
	  System.out.println("Not Found");	
	} else {
	  System.out.println("OK");
	}
        return;   
    }

    private void getversion(String filename) {
        FileInfo fileRead = metadataStub.readFile(myUtils.stringToFileInfo(filename));
	System.out.println(fileRead.getVersion());
        return;   
    }


    private static Namespace parseArgs(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("Client").build()
                .description("Client for SurfStore");
        parser.addArgument("config_file").type(String.class)
                .help("Path to configuration file");
        parser.addArgument("command").type(String.class)
                .help("Command for client to execute");
        parser.addArgument("filename").type(String.class)
                .help("Filename for Client to perform ops on"); 
        parser.addArgument("directory").type(String.class) 
                .help("Directory to store downloaded file").nargs("?").setDefault("");

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
        String command = c_args.getString("command");
        String filename = c_args.getString("filename");
        String store_dir = c_args.getString("directory");

        Client client = new Client(config);

        
        try {
            client.go();
            client.handleClient(command, filename, store_dir);
        } finally {
            client.shutdown();
        }
    }

}
