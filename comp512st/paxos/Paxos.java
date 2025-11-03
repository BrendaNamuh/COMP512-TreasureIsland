package comp512st.paxos;

import comp512.gcl.*;
import comp512.utils.*;

import java.io.*;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;

public class Paxos 
{
    private final GCL gcl;
    private final FailCheck failCheck;
    private final Logger logger;

    // Paxos state
    private final Map<Integer, Object> consensusValues = new ConcurrentHashMap<>(); // sequence number, decided value - yet to be delivered buffer
    private final Map<Integer, Integer> promiseCount = new ConcurrentHashMap<>(); // sequence number, number of promise messages received
    private final Map<Integer, Integer> acceptCount = new ConcurrentHashMap<>(); // seqeuenc number, number of accept messages received
    private final Map<Integer, Long> promisedBallotID = new ConcurrentHashMap<>(); // sequence number, highest proposal number that was promised by this process for this sequence number

    private final Map<Integer, Long> acceptedBallotID = new ConcurrentHashMap<>(); // seq number, ballotID of the latest proposal this process accepted
    private final Map<Integer, Object> acceptedValues = new ConcurrentHashMap<>(); // sequence numnber, value accepted by this process for that seq num

    private final Map<Integer, Long> highestAcceptedBallot = new ConcurrentHashMap<>(); // sequence number, highest accepted ballot sent in promise messages
    private final Map<Integer, Object> highestAcceptedValue = new ConcurrentHashMap<>(); // sequence number, value associated to highest accepted ballot in promise message

    private volatile boolean running = true; // is false if PaxosShutdown, otherwise remains true
    private Thread listenerThread; // constantly listens to messages from network
    private Thread workerThread;  // runs paxos
    private BlockingQueue<Object[]> clientRequestQueue = new LinkedBlockingQueue<>(); // requests waiting to be proposed
    private final Object broadcastLock = new Object(); // lock for broadcast
    private Object pendingLocalProposal = null; // what client wants to install

	private final String[] allGroupProcesses;
    private final String myProcess;

    private int localProposalCounter = 0;
    private static int currentSequenceNumber = 0; // slot 
    private int nextDeliverySequence = 0; // next number that can be delivered to the app

    public Paxos(String myProcess, String[] allGroupProcesses, Logger logger, FailCheck failCheck) throws IOException, UnknownHostException 
    {
        this.myProcess = myProcess;
        this.failCheck = failCheck;
        this.logger = logger;
        this.gcl = new GCL(myProcess, allGroupProcesses, null, logger);
		this.allGroupProcesses = allGroupProcesses;

        startListenerThread();
        startWorkerThread();
    }

    // =======================================
    // PUBLIC METHODS
    // =======================================

    public void broadcastTOMsg(Object val) 
    {
        Object[] valArray = formatValue(val); // will be integer, char

        synchronized (broadcastLock)
        {
            pendingLocalProposal = val;

            try 
            {
                clientRequestQueue.put(valArray);
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                return;
            }
        
            while (pendingLocalProposal != null && running)
            {
                try 
                {
                    broadcastLock.wait(); // block until consensus is reached
                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
       }
    }
    
    //Delivering messages to application. Loops until there a new consensusVal is returned
    public Object acceptTOMsg() throws InterruptedException 
    {
        while (running) 
        {
            synchronized (consensusValues) 
            {
                if (consensusValues.containsKey(nextDeliverySequence)) { // if the decided values map has the decided value for the next sequence number to deliver
                    Object value = consensusValues.get(nextDeliverySequence);
                    consensusValues.remove(nextDeliverySequence); // Free up memory since no longer need it
                    logger.info("Delivering sequence " + nextDeliverySequence + ": " + value);
                    nextDeliverySequence++;
                    return value;
                }
                consensusValues.wait(); // block until notified
            }
        }
        throw new InterruptedException("Paxos shutting down");
    }

    public void shutdownPaxos() 
    {
        running = false;
        gcl.shutdownGCL();
        if (listenerThread != null && listenerThread.isAlive()) 
        {
            listenerThread.interrupt();
        }
        synchronized (consensusValues) 
        {
            consensusValues.notifyAll();
        }
        logger.info("Paxos shutdown complete.");
    }

    // =======================================
    // THREADS
    // =======================================

    private void startListenerThread() 
    {
        listenerThread = new Thread(() -> 
        {
            while (running) 
            {
                try 
                {
                    GCMessage msg = gcl.readGCMessage();
                    processIncomingMessage(msg);
                } 
                catch (InterruptedException e) 
                {
                    Thread.currentThread().interrupt();
                    break;
                } 
                catch (Exception e) 
                {
                    if (running)
                        logger.warning("Listener thread error: " + e.getMessage());
                }
            }
        }, "PaxosListenerThread");
        listenerThread.start();
    }

    private void startWorkerThread()
    {
        workerThread = new Thread(()->
        {
            logger.info("Paxos Worker thread started. Ready to propose.");
            while (running)
            {
                try
                {
                    Object[] value = clientRequestQueue.poll(500, TimeUnit.MILLISECONDS);
                    if (value != null)
                    {
                        runPaxos(value);
                    }
                }
                catch (InterruptedException e)
                {
                    if (!running)
                    {
                        break;
                    }
                }
                catch (Exception e)
                {
                    logger.log(Level.SEVERE, "Exception in paxos worker loop. Error message: " + e);
                }
            }

        }, "PaxosWorkerThread");
        workerThread.start();
    }

    // =======================================
    // PRIVATE HELPERS
    // =======================================

    private void processIncomingMessage(GCMessage msg) 
    {
        try 
        {
            Object[] data = (Object[]) msg.val;
            String messageType = (String) data[0];

            switch (messageType) 
            {
                case "PROPOSE":
                    handlePropose(msg.senderProcess, data);
                    break;
                case "PROMISE":
                    handlePromise(msg.senderProcess, data);
                    break;
                case "ACCEPT":
                    handleAccept(msg.senderProcess, data);
                    break;
                case "ACCEPTED":
                    handleAccepted(msg.senderProcess, data);
                    break;
                case "DECIDE":
                    handleDecide(msg.senderProcess, data);
                    break; 
                default:
                    logger.warning("Unknown message type: " + messageType);
            }
        } 
        catch (Exception e) 
        {
            logger.warning("Failed to process message: " + e);
        }
    }

    private void runPaxos(Object[] value)
    {
        Object val = value;
        boolean consensus = false;
        int sequenceNum = getCurrentSequenceNumber();
        int playerNum = (int) value[0];
        int retryCount = 0;

        while (!consensus && running) 
        {
            long proposalNum = generateBallotID();
            logger.info("Player "+playerNum+" - Starting Paxos for seq=" + sequenceNum + ", proposal=" + proposalNum);

            promiseCount.remove(sequenceNum); // clear
            highestAcceptedBallot.remove(sequenceNum); 
            highestAcceptedValue.remove(sequenceNum);
            
            // Phase 1: Prepare
            boolean majorityPromised = phase1Propose(proposalNum, sequenceNum);
            if (!majorityPromised) 
            {
                logger.warning("Player " + playerNum +" - Prepare phase failed, retrying...");

                long backoffTimeMs = (long) (Math.pow(2, retryCount) * (new Random().nextInt(100) + 50)); // compute increasing delay 
                if (backoffTimeMs > 3000) backoffTimeMs = 3000; 
                retryCount++;
                try 
                {
                    Thread.sleep(backoffTimeMs);  // delay before next retry is allowed
                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                    break;
                }
                continue;
            }

            Long acceptedNum = highestAcceptedBallot.get(sequenceNum);
            Object acceptedVal = highestAcceptedValue.get(sequenceNum);

            if (acceptedVal != null) // if there is a previously accepted val, proposer must propose that one
            { 
                logger.info("Player "+ playerNum +" - Proposing previously accepted value (" + acceptedVal + ") from ballotID " + acceptedNum);
                val = acceptedVal; 
            } 
            else 
            {
                logger.info("Player "+ playerNum +" - Proposing client's value: " + ((Object[])value)[1]);
            }
            
            highestAcceptedBallot.remove(sequenceNum); // clear
            highestAcceptedValue.remove(sequenceNum);

            failCheck.checkFailure(FailCheck.FailureType.AFTERBECOMINGLEADER);

            // Phase 2: Accept
            boolean majorityAccepted = phase2Propose(proposalNum, sequenceNum, val);
            if (!majorityAccepted) 
            {
                logger.warning("Accept phase failed, retrying...");
                continue;
            }

            failCheck.checkFailure(FailCheck.FailureType.AFTERVALUEACCEPT);

            // Phase 3: Decide
            markAsConsensus(sequenceNum, val,true);
            consensus = true;
        }
    }

    // =======================================
    // PHASE 1: PROPOSE / PROMISE
    // =======================================

    private boolean phase1Propose(long proposalNum, int seqNum) 
    {
        promiseCount.put(seqNum, 0); // add sequence number, promise count to the concurrent map
        highestAcceptedBallot.remove(seqNum); // clean
        highestAcceptedValue.remove(seqNum);

        gcl.broadcastMsg(new Object[]{"PROPOSE", proposalNum, seqNum});
        return waitForMajority(promiseCount, seqNum);
    }

    private void handlePropose(String sender, Object[] data) 
    {
        long proposalNum = (long) data[1];
        int seqNum = (int) data[2];
        //Object val = data[3];

        // Get the latest promised value 
        Long promised = promisedBallotID.get(seqNum);
        //The only case  proposalNum==promised is if promisedBallotID failed to update and it's trying again
        // Otherwise no players should have the same promisedBallotID within a sequence
        if (promised == null || proposalNum >= promised) 
        {     
            // Update the promised proposal number
            promisedBallotID.put(seqNum, proposalNum);

            Long acceptedNum = acceptedBallotID.get(seqNum);
            Object acceptedValue = acceptedValues.get(seqNum);

            // Send PROMISE back to the proposer
            gcl.sendMsg(new Object[]{"PROMISE", proposalNum, seqNum, acceptedNum, acceptedValue}, sender);
        } 
        else 
        {
            // Ignore the prepare message because proposalNum is too small
            logger.fine("Ignored prepare from " + sender + " for seq " + seqNum + " with proposal " + proposalNum +
                        " (already promised " + promised + ")");
        }
    }

    private void handlePromise(String sender, Object[] data) 
    {
        int seqNum = (int) data[2];
        Long acceptedBallotID = (Long) data[3]; // accepted ballot
        Object acceptedValue = data[4]; // accepted value

        // Increment promise for this sequence
        if (promiseCount.containsKey(seqNum)) 
        {
            promiseCount.put(seqNum, promiseCount.get(seqNum) + 1);
        } 
        // Set first promise for this sequence
        else 
        {
            promiseCount.put(seqNum, 1);
        }

        Long currentHighestAccepted = highestAcceptedBallot.getOrDefault(seqNum, -1L);

        if (acceptedBallotID != null && (acceptedBallotID > currentHighestAccepted)) // if this ballot ID is greater than the current highest
        {
            highestAcceptedBallot.put(seqNum, acceptedBallotID);
            highestAcceptedValue.put(seqNum, acceptedValue);
        }
    }

    // =======================================
    // PHASE 2: ACCEPT / ACCEPTED
    // =======================================

    private boolean phase2Propose(long proposalNum, int seqNum, Object val) 
    {
        acceptCount.put(seqNum, 0);
        gcl.broadcastMsg(new Object[]{"ACCEPT", proposalNum, seqNum, val});
        return waitForMajority(acceptCount, seqNum);
    }

    private void handleAccept(String sender, Object[] data) 
    {
        long proposalNum = (long) data[1];
        int seqNum = (int) data[2];
        Object val = data[3];
        
        // Get the latest promised value 
        Long promised = promisedBallotID.get(seqNum);
    
        if (promised == null || proposalNum >= promised) // send back accepted message if value is higher than latest accepted proposal
        {
            promisedBallotID.put(seqNum, proposalNum);
            acceptedBallotID.put(seqNum, proposalNum); // add the accepted ballotID
            acceptedValues.put(seqNum, val);
            gcl.sendMsg(new Object[]{"ACCEPTED", proposalNum, seqNum, val}, sender); 
        } 
        else 
        {
            // Ignore the prepare message because proposalNum is too small
            logger.fine("Ignored accept from " + sender + " for seq " + seqNum + " with proposal " + proposalNum +
                        " (already promised " + promised + ")");
        }
    }

    private void handleAccepted(String sender, Object[] data) 
    {
        int seqNum = (int) data[2];
        
        // Increment acceptedCount for this sequence
        if (acceptCount.containsKey(seqNum)) 
        {
            acceptCount.put(seqNum, acceptCount.get(seqNum) + 1);
        } 
        // Set first promise for this sequence
        else 
        {
            acceptCount.put(seqNum, 1);
        }

    }

    // =======================================
    // PHASE 3: DECIDE / LEARN
    // =======================================

    private void handleDecide(String sender, Object[] data)
    {
        int seqNum = (int) data[2];
        Object val = data[3];
        markAsConsensus(seqNum, val,false);

        synchronized (broadcastLock)
        {
            pendingLocalProposal = null;
            broadcastLock.notifyAll();
        }
    }

    private void markAsConsensus(int seqNum, Object val, boolean isBroadcastingConsensus) // update consesus values map 
    {
        // Record decided value locally
        consensusValues.put(seqNum, val);
        synchronized (consensusValues) //synchronize keyword to enforce one thread to access this block at a time 
        { 
            if (seqNum == currentSequenceNumber)
            {
                currentSequenceNumber = seqNum + 1; // upon decision, update the next available slot 
            }
            consensusValues.notifyAll(); // wake acceptTOMsg() - this is the notification that the consensusValues has been updated
        }
        synchronized(broadcastLock)
        {
            // if (pendingLocalProposal != null && valuesDeepMatch(val, pendingLocalProposal))
            // {
                pendingLocalProposal = null;
                broadcastLock.notifyAll();
            // }
        }
        if (isBroadcastingConsensus)
        {
            gcl.broadcastMsg(new Object[]{"DECIDE",null, seqNum, val}); // 0 is just
        }
        
    }

    // =======================================
    // UTILS
    // =======================================

    private boolean waitForMajority(Map<Integer, Integer> map, int seqNum) 
    {
        //int majority = (int) Math.floor(gcl.getAllMembers().length / 2.0) + 1;
		int majority = (int) Math.floor(allGroupProcesses.length / 2.0) + 1;
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < 3000) // 3-second timeout
        { 
            if (map.getOrDefault(seqNum, 0) >= majority)
                return true;
            try 
            {
                Thread.sleep(100);
            } 
            catch (InterruptedException ignored) 
            {
                Thread.currentThread().interrupt();
                break;
            }
        }
        return false;
    }

    private synchronized long generateBallotID() 
    {
        //long processHash = Math.abs(myProcess.hashCode() % 10000); // % 10000 limits value to 4 digits
        //return (proposalCounter++ * 100000) + processHash; // * 100000 allows first 4 digits of result to represent proposalCounter and last 4 digits to represent hashCode

        long timestamp = System.currentTimeMillis(); 
        long processHash = Math.abs(myProcess.hashCode() % 10000); // tie break is process hash
        return (timestamp * 10000) + processHash;
    }

    private synchronized int getCurrentSequenceNumber() 
    {
        return currentSequenceNumber;
    }

    private boolean valuesDeepMatch(Object val1, Object val2) { // compare values instead of object identity
        if (val1 == val2) 
        {
            return true;
        }
        if (val1 == null || val2 == null) 
        {
            return false;
        }
        if (val1.getClass().isArray() && val2.getClass().isArray()) 
        {
            return Arrays.deepEquals((Object[]) val1, (Object[]) val2);
        }
        return val1.equals(val2);
    }

    private Object[] formatValue(Object value)
    {
        if (value instanceof Object[] && ((Object[]) value).length == 2)
        {
            Object[] valueArray = (Object[]) value;

            if (!(valueArray[0] instanceof Integer) && valueArray[0] instanceof String) // cast string to int safely
            {
                try
                {
                    valueArray[0] = Integer.parseInt((String) valueArray[0]);
                }
                catch(NumberFormatException e)
                {
                    logger.warning("Cannot parse first message element to integer: " + valueArray[0]);
                }
            }

            if (!(valueArray[1] instanceof Character) && valueArray[1] instanceof String)
            {
                String s = (String)valueArray[1];
                if (!s.isEmpty())
                {
                    valueArray[1] = s.charAt(0);
                }
            }
            return valueArray;
        }

        return new Object[]{null, value};

    }
}

