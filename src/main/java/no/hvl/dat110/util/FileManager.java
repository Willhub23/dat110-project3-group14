package no.hvl.dat110.util;

/**
 * @author tdoy
 * dat110 - project 3
 */

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.rmi.RemoteException;
import java.security.NoSuchAlgorithmException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import no.hvl.dat110.middleware.Message;
import no.hvl.dat110.rpc.interfaces.NodeInterface;

public class FileManager {

	private static final Logger logger = LogManager.getLogger(FileManager.class);

	private BigInteger[] replicafiles; // array stores replicated files for distribution to matching nodes
	private int numReplicas; // let's assume each node manages nfiles (5 for now) - can be changed from the
								// constructor
	private NodeInterface chordnode;
	private String filepath; // absolute filepath
	private String filename; // only filename without path and extension
	private BigInteger hash;
	private byte[] bytesOfFile;
	private String sizeOfByte;

	private Set<Message> activeNodesforFile = null;

	public FileManager(NodeInterface chordnode) throws RemoteException {
		this.chordnode = chordnode;
	}

	public FileManager(NodeInterface chordnode, int N) throws RemoteException {
		this.numReplicas = N;
		replicafiles = new BigInteger[N];
		this.chordnode = chordnode;
	}

	public FileManager(NodeInterface chordnode, String filepath, int N) throws RemoteException {
		this.filepath = filepath;
		this.numReplicas = N;
		replicafiles = new BigInteger[N];
		this.chordnode = chordnode;
	}

	public void createReplicaFiles() {

		// set a loop where size = numReplicas
		

		// Loop numReplicas times
		for (int i = 0; i < numReplicas; i++) {

			// Generate replica file name by appending the index to the file name
			String replicaName = filename + i;

			// Hash the replica file name using the hashOf() method from the Hash class
			BigInteger replicaHash = Hash.hashOf(replicaName);

			// Store the hash in the replicafiles array
			replicafiles[i] = replicaHash;
		}
	}

	/**
	 * 
	 * @param bytesOfFile
	 * @throws RemoteException
	 */
	public int distributeReplicastoPeers() throws RemoteException {

		// randomly appoint the primary server to this file replicas
		Random rnd = new Random();
		int index = rnd.nextInt(Util.numReplicas - 1);

		int counter = 0;

		// Task1: Given a filename, make replicas and distribute them to all active
		// peers such that: pred < replica <= peer
		createReplicaFiles();
		
		NodeInterface primaryNode = chordnode.findSuccessor(hash);

		// Task3: for each replica, find its successor and call the addKey on the successor
		for (int i = 0; i < numReplicas; i++) {

		    // get the replica's id
		    BigInteger replicaID = replicafiles[i];

		    // find the successor of the replica
		    NodeInterface successorNode = primaryNode.findSuccessor(replicaID);

		    // add the replica to the successor node
		    boolean added = false;
		    try {
		        successorNode.addKey(replicaID);
		        added = true;
		    } catch (RemoteException e) {
		        // Replica could not be added to the node
		    }
		    boolean isPrimary = index == counter;
		    
		    successorNode.saveFileContent(filename, replicafiles[i], bytesOfFile, isPrimary);

		    // if the replica was successfully added to the node, increment the counter
		    if (added) {
		        counter++;
		    }
		    
		
		}
		NodeInterface primary = null;

	    // iterate over the replicas
	    for (BigInteger replica : replicafiles) {

	        // find the successor for the replica
	        NodeInterface successor = chordnode.findSuccessor(replica);

	        // add the replica to the successor's replica set
	        successor.addKey(replica);

	        counter++;
	    }

	    return counter;
	}


	

	/**
	 * 
	 * @param filename
	 * @return list of active nodes having the replicas of this file
	 * @throws RemoteException
	 */
	
	public Set<Message> requestActiveNodesForFile(String filename) throws RemoteException {
		this.filename = filename;
		activeNodesforFile = new HashSet<Message>(); 

		// Task: Given a filename, find all the peers that hold a copy of this file
		
		// generate the N replicas from the filename by calling createReplicaFiles()
		createReplicaFiles();
		
		// iterate over the replicas of the file
		for (int i = 0; i < replicafiles.length; i++) {
			// for each replica, do findSuccessor(replica) that returns successor s.
	        NodeInterface successor = chordnode.findSuccessor(replicafiles[i]);

	        // get the metadata (Message) of the replica from the successor (i.e., active peer) of the file
	        Message m = successor.getFilesMetadata().get(replicafiles[i]);
	        
	        // save the metadata in the set activeNodesforFile.
	        if(m != null) {
	        	activeNodesforFile.add(m);
	        }
			
	    }
		
		return activeNodesforFile;
	}

	/**
	 * Find the primary server - Remote-Write Protocol
	 * 
	 * @return
	 * @throws RemoteException 
	 */
	public NodeInterface findPrimaryOfItem() throws RemoteException {

		// Task: Given all the active peers of a file (activeNodesforFile()), find which
		// is holding the primary copy

		// iterate over the activeNodesforFile

		// for each active peer (saved as Message)

		// use the primaryServer boolean variable contained in the Message class to
		// check if it is the primary or not

		// return the primary when found (i.e., use Util.getProcessStub to get the stub
		// and return it)
		
		for(int i = 0; i < replicafiles.length; i++) {
			NodeInterface succ = chordnode.findSuccessor(replicafiles[i]);
			
			Message m = succ.getFilesMetadata(replicafiles[i]);
			
			if(m.isPrimaryServer()) {
				return Util.getProcessStub(succ.getNodeName(), succ.getPort());
			}
		}

		return null;
	}

	/**
	 * Read the content of a file and return the bytes
	 * 
	 * @throws IOException
	 * @throws NoSuchAlgorithmException
	 */
	public void readFile() throws IOException, NoSuchAlgorithmException {

		File f = new File(filepath);

		byte[] bytesOfFile = new byte[(int) f.length()];

		FileInputStream fis = new FileInputStream(f);

		fis.read(bytesOfFile);
		fis.close();

		// set the values
		filename = f.getName().replace(".txt", "");
		hash = Hash.hashOf(filename);
		this.bytesOfFile = bytesOfFile;
		double size = (double) bytesOfFile.length / 1000;
		NumberFormat nf = new DecimalFormat();
		nf.setMaximumFractionDigits(3);
		sizeOfByte = nf.format(size);

		logger.info("filename=" + filename + " size=" + sizeOfByte);

	}

	public void printActivePeers() {

		activeNodesforFile.forEach(m -> {
			String peer = m.getNodeName();
			String id = m.getNodeID().toString();
			String name = m.getNameOfFile();
			String hash = m.getHashOfFile().toString();
			int size = m.getBytesOfFile().length;

			logger.info(
					peer + ": ID = " + id + " | filename = " + name + " | HashOfFile = " + hash + " | size =" + size);

		});
	}

	/**
	 * @return the numReplicas
	 */
	public int getNumReplicas() {
		return numReplicas;
	}

	/**
	 * @return the filename
	 */
	public String getFilename() {
		return filename;
	}

	/**
	 * @param filename the filename to set
	 */
	public void setFilename(String filename) {
		this.filename = filename;
	}

	/**
	 * @return the hash
	 */
	public BigInteger getHash() {
		return hash;
	}

	/**
	 * @param hash the hash to set
	 */
	public void setHash(BigInteger hash) {
		this.hash = hash;
	}

	/**
	 * @return the bytesOfFile
	 */
	public byte[] getBytesOfFile() {
		return bytesOfFile;
	}

	/**
	 * @param bytesOfFile the bytesOfFile to set
	 */
	public void setBytesOfFile(byte[] bytesOfFile) {
		this.bytesOfFile = bytesOfFile;
	}

	/**
	 * @return the size
	 */
	public String getSizeOfByte() {
		return sizeOfByte;
	}

	/**
	 * @param size the size to set
	 */
	public void setSizeOfByte(String sizeOfByte) {
		this.sizeOfByte = sizeOfByte;
	}

	/**
	 * @return the chordnode
	 */
	public NodeInterface getChordnode() {
		return chordnode;
	}

	/**
	 * @return the activeNodesforFile
	 */
	public Set<Message> getActiveNodesforFile() {
		return activeNodesforFile;
	}

	/**
	 * @return the replicafiles
	 */
	public BigInteger[] getReplicafiles() {
		return replicafiles;
	}

	/**
	 * @param filepath the filepath to set
	 */
	public void setFilepath(String filepath) {
		this.filepath = filepath;
	}
}
