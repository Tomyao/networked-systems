package surfstore;

import java.nio.charset.StandardCharsets; 
import java.security.MessageDigest; 
import java.security.NoSuchAlgorithmException; 

import java.io.UnsupportedEncodingException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.FileNotFoundException;
import com.google.protobuf.ByteString;
import surfstore.SurfStoreBasic.Block;
import surfstore.SurfStoreBasic.SimpleAnswer;
import surfstore.SurfStoreBasic.Empty;
import surfstore.SurfStoreBasic.FileInfo;

import java.util.*;

public class myUtils {
   
    //Function that returns whether file exists
    public static boolean fileExists(String filepath) {
        boolean exists = false;
        // Tests for existence of file/directory
        File tmpDir = new File(filepath);
        exists = tmpDir.exists();
        // Make sure it is a file instead of a directory
        if (tmpDir.isDirectory()) {
            exists = false;
        }
        return exists;
    }
    
    //Function that gets file from disk and decomposes into blocks
    public static ArrayList<Block> fileToBlockList (String filepath) {
        ArrayList<Block> myBlocks = new ArrayList<Block>();
        FileInputStream myInputStream = null;
        
        try {
            // Get file from disk
            myInputStream = new FileInputStream(filepath);
            // Block size of 4kb
            byte[] buffer = new byte[4096];
            // For each block, call byteArrayToBlock
            int read = 0;
            while( ( read = myInputStream.read( buffer ) ) > 0 ){
                if (read < 4096) {
                    buffer = Arrays.copyOfRange(buffer, 0, read);
                }
                Block.Builder builder = Block.newBuilder();
                builder.setData(ByteString.copyFrom(buffer));
                builder.setHash(genHash(buffer));
                myBlocks.add(builder.build());
            }
        } catch (FileNotFoundException e) {
            System.err.println("File Not Found");
        } catch (IOException e) {
            System.err.println("IOEXCeption");
        } finally {
            try {
                myInputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        
        return myBlocks;
    }
    
    //Function that writes blocks to file
    public static void blockListToFile(ArrayList<Block> myBlocks, String filepath) {
        FileOutputStream myOutputStream = null;
        
        try {
            myOutputStream = new FileOutputStream(filepath);
            for (Block currBlock: myBlocks) {
                myOutputStream.write(currBlock.getData().toByteArray());
            }
        //} catch (FileNotFoundException e) {
        //    System.err.println("FileNotFound");
        } catch (IOException e) {
            System.err.println("IOEXCeption");
        } finally {
            try {
                myOutputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    //Turn BlockList into a HashList
    public static List<String> blocksToHashList (ArrayList<Block> myBlocks) {
        List<String> myHashList = new ArrayList<String>();
        for (int i = 0; i < myBlocks.size(); i++) {
            myHashList.add(myBlocks.get(i).getHash());
        }
        return myHashList;
    }

    public static FileInfo stringToFileInfo(String s) {
        return FileInfo.newBuilder()
                       .setFilename(s)
                       .build();
    }

    public static FileInfo createFileInfo(String s, int v, List<String> hl) {
        FileInfo.Builder builder  = FileInfo.newBuilder();

        builder.setFilename(s);
        builder.setVersion(v);
        if (hl != null) {
            builder.addAllBlocklist(hl);
        }
        return builder.build(); //turns Builder into a FileInfo
    }

    // Build block from string specified
    public static Block hashToBlock(String s) {
        Block.Builder builder  = Block.newBuilder();

        //copy grom turns a bytearray to bytestring
        try {  
            builder.setData(ByteString.copyFrom(s, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        builder.setHash(s);
        return builder.build(); //turns Builder into a Block 
    }

    //Compute hash for each string block  
    public static String genHash(String s) {
        // Note : check htat arg is a string? valid data type? 
        MessageDigest digest = null; 
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace(); 
            System.exit(2);
        }
        
        byte[] hash = digest.digest(s.getBytes(StandardCharsets.UTF_8));
        String encoded = Base64.getEncoder().encodeToString(hash);
        return encoded; 
    }
    
    //Compute hash for byte array
    public static String genHash(byte[] myBytes) {
        MessageDigest digest = null;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            System.exit(2);
        }
        
        byte[] hash = digest.digest(myBytes);
        String encoded = Base64.getEncoder().encodeToString(hash);
        return encoded;
    }

}

