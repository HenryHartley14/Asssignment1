/*
 * Replace the following string of 0s with your student number
 * 240502533
 */
import java.io.File;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.net.DatagramPacket;
import java.io.*;
import java.net.SocketTimeoutException;





public class Protocol {

	static final String  NORMAL_MODE="nm"   ;         // normal transfer mode: (for Part 1 and 2)
	static final String	 TIMEOUT_MODE ="wt"  ;        // timeout transfer mode: (for Part 3)
	static final String	 LOST_MODE ="wl"  ;           // lost Ack transfer mode: (for Part 4)
	static final int DEFAULT_TIMEOUT =1000  ;         // default timeout in milliseconds (for Part 3)
	static final int DEFAULT_RETRIES =4  ;            // default number of consecutive retries (for Part 3)
	public static final int MAX_Segment_SIZE = 4096;  //the max segment size that can be used when creating the received packet's buffer

	/*
	 * The following attributes control the execution of the transfer protocol and provide access to the 
	 * resources needed for the transfer 
	 * 
	 */ 

	private InetAddress ipAddress;      // the address of the server to transfer to. This should be a well-formed IP address.
	private int portNumber; 		    // the  port the server is listening on
	private DatagramSocket socket;      // the socket that the client binds to

	private File inputFile;            // the client-side CSV file that has the readings to transfer  
	private String outputFileName ;    // the name of the output file to create on the server to store the readings
	private int maxPatchSize;		   // the patch size - no of readings to be sent in the payload of a single Data segment

	private Segment dataSeg   ;        // the protocol Data segment for sending Data segments (with payload read from the csv file) to the server 
	private Segment ackSeg  ;          // the protocol Ack segment for receiving ACK segments from the server

	private int timeout;              // the timeout in milliseconds to use for the protocol with timeout (for Part 3)
	private int maxRetries;           // the maximum number of consecutive retries (retransmissions) to allow before exiting the client (for Part 3)(This is per segment)
	private int currRetry;            // the current number of consecutive retries (retransmissions) following an Ack loss (for Part 3)(This is per segment)

	private int fileTotalReadings;    // number of all readings in the csv file
	private int sentReadings;         // number of readings successfully sent and acknowledged
	private int totalSegments;        // total segments that the client sent to the server

	// Shared Protocol instance so Client and Server access and operate on the same values for the protocol’s attributes (the above attributes).
	public static Protocol instance = new Protocol();

	/**************************************************************************************************************************************
	 **************************************************************************************************************************************
	 * For this assignment, you have to implement the following methods:
	 *		sendMetadata()
	 *      readandSend()
	 *      receiveAck()
	 *      startTimeoutWithRetransmission()
	 *		receiveWithAckLoss()
	 * Do not change any method signatures, and do not change any other methods or code provided.
	 ***************************************************************************************************************************************
	 **************************************************************************************************************************************/
	/* 
	 * This method sends protocol metadata to the server.
	 * See coursework specification for full details.	
	 */
	public void sendMetadata() throws IOException  { 
		int count = 0;
		//opening file to be read
		FileReader in = new FileReader("data.csv");
		BufferedReader br = new BufferedReader(in);

		while (br.readLine() != null){
			//counting how many lines are in the file
			count = count + 1;

		}

		instance.fileTotalReadings = count;
		int patchSize = instance.maxPatchSize;
		String outputFileName = instance.outputFileName;


		//making meta segment
		String payload = count + "," + outputFileName + "," + patchSize;
		Segment metaSeg = new Segment(0, SegmentType.Meta, payload, payload.getBytes().length);

		//Serialize segment object into a byte array
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		ObjectOutputStream os = new ObjectOutputStream(outputStream);
		os.writeObject(metaSeg);
		os.flush();
		byte[] sendData = outputStream.toByteArray();

		//Create DatagramPacket 
		DatagramPacket packet = new DatagramPacket(
			sendData,
			sendData.length,
			instance.ipAddress,
			instance.portNumber
		);

		instance.socket.send(packet);


		
		System.out.println("CLIENT: Sending meta data");
    	System.out.println("CLIENT: META [SEQ#0] (Number of readings: " + count 
                       + ", file name: " + outputFileName 
                       + ", patch size: " + patchSize + ")");

		br.close();


	} 


	/* 
	 * This method read and send the next data segment (dataSeg) to the server. 
	 * See coursework specification for full details.
	 */
	public void readAndSend() throws IOException {
		FileReader in = new FileReader("data.csv");
		BufferedReader br = new BufferedReader(in);
		String line;

		//skip sent readings
		int linesToSkip = instance.sentReadings;
		for (int i = 0; i < linesToSkip; i++){
			br.readLine();
		}

		StringBuilder payloadBuilder = new StringBuilder();


		int count = 0;

			//read up to the max patchsize of lines
		while (count < instance.maxPatchSize && (line = br.readLine()) != null){
			//split lines into individual values
			String[] parts = line.split(",");
			String sensorId = parts[0];
			long timestamp = Long.parseLong(parts[1]);
			float[] values = new float[]{
				Float.parseFloat(parts[2]),		//temp
				Float.parseFloat(parts[3]),		//humidity
				Float.parseFloat(parts[4])		//pressure
			};

			//create reading object
			Reading reading = new Reading(sensorId, timestamp, values);  //creating reading object for line
				
			if (count>0) payloadBuilder.append(";");   //seperate readings with ;
			payloadBuilder.append(reading.toString());
			count++;



		}
			
		if (count == 0){
			System.out.println("Total Segments: " + instance.totalSegments);
			System.exit(0); // stop if there are no more lines

		}	
		int seqNum = 1- ((instance.sentReadings / instance.maxPatchSize) % 2); //alternate seqnum between 1 and 0
			
		String payload = payloadBuilder.toString();//make payload a string
		Segment dataSeg = new Segment(seqNum, SegmentType.Data, payload, payload.length());
		instance.dataSeg = dataSeg;
		
		
		
		//send segment to the server
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		ObjectOutputStream os = new ObjectOutputStream(outputStream);
		os.writeObject(dataSeg);
		os.flush();
		byte[] sendData = outputStream.toByteArray();

		DatagramPacket packet = new DatagramPacket(
				sendData, sendData.length, instance.ipAddress, instance.portNumber
			);


		instance.socket.send(packet);
		instance.totalSegments++;

		System.out.println("CLIENT: Send: DATA [SEQ#" + seqNum + "] (size:" + payload.length() + ", crc:" + dataSeg.getChecksum()+ ", content:" + payload + ")");



		br.close();
		in.close();

	
	}
	/* 
	 * This method receives the current Ack segment (ackSeg) from the server 
	 * See coursework specification for full details.
	 */
	public boolean receiveAck() throws IOException { 
		

		byte[] buffer = new byte[Protocol.MAX_Segment_SIZE]; //prepare the buffer to have the max segment size
		DatagramPacket incomingPacket = new DatagramPacket(buffer, buffer.length); // create datagrampacket to recieve
		instance.socket.receive(incomingPacket);

		//turn received bytes back into segment 
		ByteArrayInputStream in = new ByteArrayInputStream(incomingPacket.getData());
		ObjectInputStream is = new ObjectInputStream(in); // convert Bytes to Segment

		Segment ackSeg = null;
		int expectedSeqNum = 1 - ((instance.sentReadings / instance.getMaxPatchSize()) % 2);

		try{
			ackSeg = (Segment) is.readObject();
		}

		catch(ClassNotFoundException e){
			e.printStackTrace();
			return true;
		}
		//check if received segment is an ack
		if(ackSeg.getType() == SegmentType.Ack){
			System.out.println("CLIENT: Received ACK [SEQ#" + ackSeg.getSeqNum() + "]");
			System.out.println("**************************************************************************");


			// check seqnum
			if(ackSeg.getSeqNum() != expectedSeqNum){
				System.out.println("Client: Recieved Wrong ACK Sequence. Expected" + expectedSeqNum + ",got " + ackSeg.getSeqNum());
				return false;
			}

			instance.sentReadings += instance.getMaxPatchSize();

			if(instance.sentReadings > instance.getFileTotalReadings()){
				System.out.print("Client: Transfer complete, Total Segments sent: " + Protocol.instance.getFileTotalReadings());
				System.exit(0);
			}

			return true;
		
		
		
		}




		return false;
	}

	/* 
	 * This method starts a timer and does re-transmission of the Data segment 
	 * See coursework specification for full details.
	 */
	public void startTimeoutWithRetransmission() throws IOException  { 
		instance.socket.setSoTimeout(instance.timeout);
	

		while (true){
			try{
				if(receiveAck()){ //Ack recieved 
					instance.currRetry = 0; // reset retry count
					return;
				}
				
			} catch(SocketTimeoutException e){
				instance.currRetry = instance.currRetry + 1; //increment retry counter 

				if(instance.currRetry > instance.maxRetries){ //check if max retries exceeded
					System.out.println("Client: Max retries exceeded");
					System.exit(1);

				}

				System.out.println("Client: TIMEOUT ALERT"); 
				System.out.println("CLient: Re-sending the same segment again, current retry " + instance.currRetry);//if max retries not exeeded continue with retransmission

				//Resent dataseg
				ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
				ObjectOutputStream os = new ObjectOutputStream(outputStream);
				os.writeObject(instance.dataSeg); // use existing segment
				os.flush();
				byte[] sendData = outputStream.toByteArray();

				DatagramPacket packet = new DatagramPacket(
					sendData, sendData.length, instance.ipAddress, instance.portNumber
				);


				instance.socket.send(packet);

				instance.totalSegments = instance.totalSegments + 1;

				System.out.println("Client: Retransmitting data [SeqNum:" + instance.dataSeg.getSeqNum() + "]");
			}
		}


	}


	/* 
	 * This method is used by the server to receive the Data segment in Lost Ack mode
	 * See coursework specification for full details.
	 */
	public void receiveWithAckLoss(DatagramSocket serverSocket, float loss)  {
		int expectedSeqNum = 1;
		int lastReceivedSeqNum = -1;
		int totalBytesReceived = 0;
		int usefulBytesReceived = 0;
		ArrayList<String> tempDataList = new ArrayList<>();

		try{
			serverSocket.setSoTimeout(2000);
		} catch (SocketException e){
			System.out.println("Error setting socket timeout: " + e.getMessage());
			return;

		}

		System.out.println("Server ready to receive with ACK loss simulation");

		while (true){
			try{
				byte[] buffer = new byte[MAX_Segment_SIZE];
				DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

				serverSocket.receive(packet); // recieve packet

				Segment segment = null; // deserialize the segment 

				try{
					ByteArrayInputStream in = new ByteArrayInputStream(packet.getData(), 0, packet.getLength());
					ObjectInputStream is = new ObjectInputStream(in);
					segment = (Segment) is.readObject();
					is.close();
				} catch(ClassNotFoundException | IOException e){
					System.out.println("Couldnt deserializing segment: " + e.getMessage());
					continue;
				}

				InetAddress clientAddress = packet.getAddress(); // get client adress to send ack
				int clientPort = packet.getPort();

				//extract segment fields
				int seqNum = segment.getSeqNum();
				long checksum = segment.getChecksum();
				String data = segment.getPayLoad();
				int dataLength = segment.getSize();

				if (!segment.isValid()){ //validate checksum
					System.out.println("Server: Currupted segment [SeqNum: " + seqNum + "] ");
					continue;
				}

				totalBytesReceived = totalBytesReceived + dataLength;

				if (seqNum == expectedSeqNum){
					System.out.println("---------------------------------");
					System.out.println("Server: Recieved new Segment [SeqNum: " + seqNum + "size: " + dataLength + ", crc:" + checksum + ", content:" + data + "]");

					tempDataList.add(data);

					usefulBytesReceived += dataLength;

					lastReceivedSeqNum = seqNum;
					expectedSeqNum = 1 - expectedSeqNum;

					if(!isLost(loss)){ // send ack with loss simulation
						Server.sendAck(serverSocket, clientAddress, clientPort, seqNum);
						System.out.println("Server: Ack sent for SeqNum: " + seqNum);
					} else{
						System.out.println("Server: Ack lost for SeqNum: " + seqNum);
					}




				} else{
					System.out.println("Server: Duplicate deteced [SeqNum: " + seqNum + "]");

					if (lastReceivedSeqNum >= 0){
						if(!isLost(loss)){
							Server.sendAck(serverSocket, clientAddress, clientPort, lastReceivedSeqNum);
							System.out.println("Server: Resending ack for: " + lastReceivedSeqNum);

						} else{
							System.out.println("Server: Ack lost for resent ack");
						}
					}
				}
			} catch(SocketTimeoutException e){
				System.out.println("Server: Timeout");

				if(!tempDataList.isEmpty()){
					try{
						Server.writeReadingsToFile(tempDataList, instance.outputFileName);
					} catch(IOException ioExecption){
						System.out.println("Server: error writing to file: " + ioExecption.getMessage());
					}
				}

				if(totalBytesReceived>0){
					double efficiency = (usefulBytesReceived / (double)totalBytesReceived) * 100;
					System.out.printf("Server: Total bytes recieved: %d%n", totalBytesReceived);
					System.out.printf("Server: useful bytes: %d%n", usefulBytesReceived);
					System.out.println("Server: efficiency: " + efficiency + "%" );
				}

				break;
			}catch(IOException e){
				System.out.println("Server: IO error: " + e.getMessage());
				break;
			}
		}

		System.exit(0);
	}


	/*************************************************************************************************************************************
	 **************************************************************************************************************************************
	 **************************************************************************************************************************************
	These methods are implemented for you .. Do NOT Change them 
	 **************************************************************************************************************************************
	 **************************************************************************************************************************************
	 **************************************************************************************************************************************/	 
	/* 
	 * This method initialises ALL the 14 attributes needed to allow the Protocol methods to work properly
	 */
	public void initProtocol(String hostName , String portNumber, String fileName, String outputFileName, String batchSize) throws UnknownHostException, SocketException {
		instance.ipAddress = InetAddress.getByName(hostName);
		instance.portNumber = Integer.parseInt(portNumber);
		instance.socket = new DatagramSocket();

		instance.inputFile = checkFile(fileName); //check if the CSV file does exist
		instance.outputFileName =  outputFileName;
		instance.maxPatchSize= Integer.parseInt(batchSize);

		instance.dataSeg = new Segment(); //initialise the data segment for sending readings to the server
		instance.ackSeg = new Segment();  //initialise the ack segment for receiving Acks from the server

		instance.fileTotalReadings = 0; 
		instance.sentReadings=0;
		instance.totalSegments =0;

		instance.timeout = DEFAULT_TIMEOUT;
		instance.maxRetries = DEFAULT_RETRIES;
		instance.currRetry = 0;		 
	}


	/* 
	 * check if the csv file does exist before sending it 
	 */
	private static File checkFile(String fileName)
	{
		File file = new File(fileName);
		if(!file.exists()) {
			System.out.println("CLIENT: File does not exists"); 
			System.out.println("CLIENT: Exit .."); 
			System.exit(0);
		}
		return file;
	}

	/* 
	 * returns true with the given probability to simulate network errors (Ack loss)(for Part 4)
	 */
	private static Boolean isLost(float prob) 
	{ 
		double randomValue = Math.random();  //0.0 to 99.9
		return randomValue <= prob;
	}

	/* 
	 * getter and setter methods	 *
	 */
	public String getOutputFileName() {
		return outputFileName;
	} 

	public void setOutputFileName(String outputFileName) {
		this.outputFileName = outputFileName;
	} 

	public int getMaxPatchSize() {
		return maxPatchSize;
	} 

	public void setMaxPatchSize(int maxPatchSize) {
		this.maxPatchSize = maxPatchSize;
	} 

	public int getFileTotalReadings() {
		return fileTotalReadings;
	} 

	public void setFileTotalReadings(int fileTotalReadings) {
		this.fileTotalReadings = fileTotalReadings;
	}

	public void setDataSeg(Segment dataSeg) {
		this.dataSeg = dataSeg;
	}

	public void setAckSeg(Segment ackSeg) {
		this.ackSeg = ackSeg;
	}

	public void setCurrRetry(int currRetry) {
		this.currRetry = currRetry;
	}

}
