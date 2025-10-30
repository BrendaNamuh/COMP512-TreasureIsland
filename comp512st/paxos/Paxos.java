package comp512st.paxos;

import comp512.gcl.*;
import comp512.utils.*;

import java.io.*;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;

public class Paxos {

    private final GCL gcl;
    private final FailCheck failCheck;
    private final Logger logger;

    // Paxos state
    private final Map<Integer, Object> consensusValues = new ConcurrentHashMap<>(); // Decided but not yet delivered value buffer
    private final Map<Integer, Integer> promiseCount = new ConcurrentHashMap<>();
    private final Map<Integer, Integer> acceptCount = new ConcurrentHashMap<>();
    private Map<Integer, Long> promisedProposalNumbers = new HashMap<>(); 


    private volatile boolean running = true; // is false if PaxosShutdown, otherwise remains true
    private Thread listenerThread;
    private final String leaderProcess;
    private final String myProcess;

    private static int proposalCounter = 0;
    private static int nextSequenceNumber = 0;
    private int nextDeliverySequence = 0;

    public Paxos(String myProcess, String[] allGroupProcesses, Logger logger, FailCheck failCheck)
            throws IOException, UnknownHostException {
        this.myProcess = myProcess;
        this.leaderProcess = allGroupProcesses[0];
        this.failCheck = failCheck;
        this.logger = logger;
        this.gcl = new GCL(myProcess, allGroupProcesses, null, logger);

        startListenerThread();
    }

    // =======================================
    // PUBLIC METHODS
    // =======================================

    public void broadcastTOMsg(Object val) {

        //If process is not leader, send move to leader as client_request 
        //Leader will be only process actually running paxos.
        if (!myProcess.equals(leaderProcess)){
            gcl.sendMsg(new Object[]{"CLIENT_REQUEST", val}, leaderProcess);
            logger.info("Forwarded message to leader " + leaderProcess);
            return;
        }

        int sequenceNum = getNextSequenceNumber();
        boolean consensus = false;
        int playerNum = val[0]

        while (!consensus && running) {
            long proposalNum = generateProposalNumber();
            logger.info("Player "+playerNum+" - Starting Paxos for seq=" + sequenceNum + ", proposal=" + proposalNum);

            // Phase 1: Prepare
            boolean majorityPromised = phase1Prepare(proposalNum, sequenceNum, val);
            if (!majorityPromised) {
                logger.warning("Player " + playerNum +" - Prepare phase failed, retrying...");
                continue;
            }

            failCheck.checkFailure(FailCheck.FailureType.AFTERBECOMINGLEADER);

            // Phase 2: Accept
            boolean majorityAccepted = phase2Propose(proposalNum, sequenceNum, val);
            if (!majorityAccepted) {
                logger.warning("Accept phase failed, retrying...");
                continue;
            }

            failCheck.checkFailure(FailCheck.FailureType.AFTERVALUEACCEPT);

            // Phase 3: Decide
            markAsConsensus(sequenceNum, val,true);
            consensus = true;
        }
    }
    
    //Delivering messages to application. Loops until there a new consensusVal is returned
    public Object acceptTOMsg() throws InterruptedException {
        while (running) {
            synchronized (consensusValues) {
                if (consensusValues.containsKey(nextDeliverySequence)) {
                    Object value = consensusValues.get(nextDeliverySequence);
                    consensusValues.remove(nextDeliverySequence); /*Free up memory since no longer need it*/
                    logger.info("Delivering sequence " + nextDeliverySequence + ": " + value);
                    nextDeliverySequence++;
                    return value;
                }
                consensusValues.wait(); // block until notified
            }
        }
        throw new InterruptedException("Paxos shutting down");
    }

    public void shutdownPaxos() {
        running = false;
        gcl.shutdownGCL();
        if (listenerThread != null && listenerThread.isAlive()) {
            listenerThread.interrupt();
        }
        synchronized (consensusValues) {
            consensusValues.notifyAll();
        }
        logger.info("Paxos shutdown complete.");
    }

    // =======================================
    // PRIVATE HELPERS
    // =======================================

    private void startListenerThread() {
        listenerThread = new Thread(() -> {
            while (running) {
                try {
                    GCMessage msg = gcl.readGCMessage();
                    processIncomingMessage(msg);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    if (running)
                        logger.warning("Listener thread error: " + e.getMessage());
                }
            }
        }, "PaxosListenerThread");
        listenerThread.start();
    }

    private void processIncomingMessage(GCMessage msg) {
        try {
            Object[] data = (Object[]) msg.val;
            String messageType = (String) data[0];
            switch (messageType) {
                case "PREPARE":
                    handlePrepare(msg.senderProcess, data);
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
                case "CLIENT_REQUEST":
                    handleClientRequest(msg.senderProcess, data);
                    break;
                default:
                    logger.warning("Unknown message type: " + messageType);
            }
        } catch (Exception e) {
            logger.warning("Failed to process message: " + e);
        }
    }


    private void handleClientRequest(String sender, Object[] data) {
        Object val = data[1];
        logger.info("Leader received client request from " + sender + ": " + val);
        broadcastTOMsg(val); // run Paxos for this value
}


    // =======================================
    // PHASE 1: PREPARE / PROMISE
    // =======================================

    private boolean phase1Prepare(long proposalNum, int seqNum, Object val) {
        promiseCount.put(seqNum, 0);
        gcl.broadcastMsg(new Object[]{"PREPARE", proposalNum, seqNum, val});
        return waitForMajority(promiseCount, seqNum);
    }

    private void handlePrepare(String sender, Object[] data) {
        long proposalNum = (long) data[1];
        int seqNum = (int) data[2];
        Object val = data[3];

        // Get the latest promised value 
        Long promised = promisedProposalNumbers.get(seqNum);
        //The only case  proposalNum==promised is if promisedProposalNumbers failed to update and it's trying again
        // Otherwise no players should have the same promisedProposalNumbers within a sequence
        if (promised == null || proposalNum >= promised) {
            
            // Update the promised proposal number
            promisedProposalNumbers.put(seqNum, proposalNum);

            // Send PROMISE back to the proposer
            gcl.sendMsg(new Object[]{"PROMISE", proposalNum, seqNum, val}, sender);
        } 
        else {
            // Ignore the prepare message because proposalNum is too small
            logger.fine("Ignored prepare from " + sender + " for seq " + seqNum + " with proposal " + proposalNum +
                        " (already promised " + promised + ")");
    }
    }

    private void handlePromise(String sender, Object[] data) {
        int seqNum = (int) data[2];
        // Increment promise for this sequence
        if (promiseCount.containsKey(seqNum)) {
            promiseCount.put(seqNum, promiseCount.get(seqNum) + 1);
        } 
        // Set first promise for this sequence
        else {
            promiseCount.put(seqNum, 1);
        }

    }

    // =======================================
    // PHASE 2: ACCEPT / ACCEPTED
    // =======================================

    private boolean phase2Propose(long proposalNum, int seqNum, Object val) {
        acceptCount.put(seqNum, 0);
        gcl.broadcastMsg(new Object[]{"ACCEPT", proposalNum, seqNum, val});
        return waitForMajority(acceptCount, seqNum);
    }

    private void handleAccept(String sender, Object[] data) {
        long proposalNum = (long) data[1];
        int seqNum = (int) data[2];
        Object val = data[3];
        
        // Get the latest promised value 
        Long promised = promisedProposalNumbers.get(seqNum);
    
        if (promised == null || proposalNum >= promised) {
            gcl.sendMsg(new Object[]{"ACCEPTED", proposalNum, seqNum, val}, sender);
        } 
        else {
            // Ignore the prepare message because proposalNum is too small
            logger.fine("Ignored accept from " + sender + " for seq " + seqNum + " with proposal " + proposalNum +
                        " (already promised " + promised + ")");
    }

        

    }

    private void handleAccepted(String sender, Object[] data) {
        int seqNum = (int) data[2];
        
        // Increment acceptedCount for this sequence
        if (acceptCount.containsKey(seqNum)) {
            acceptCount.put(seqNum, acceptCount.get(seqNum) + 1);
        } 
        // Set first promise for this sequence
        else {
            acceptCount.put(seqNum, 1);
        }

    }

    // =======================================
    // PHASE 3: DECIDE / LEARN
    // =======================================

    private void markAsConsensus(int seqNum, Object val, boolean isBroadcastingConsensus) {
        // Record decided value locally
        consensusValues.put(seqNum, val);
        synchronized (consensusValues) { //synchronize keyword to enforce one thread to access this block at a time 
            //consensusValues.wait();  puts thread to sleep until notified
            consensusValues.notifyAll(); // wake acceptTOMsg() // this is the notification that the consensusValues has been updated
        }
        if (isBroadcastingConsensus){
            gcl.broadcastMsg(new Object[]{"DECIDE",null, seqNum, val}); // 0 is just
        }
        
    }

    private void handleDecide(String sender, Object[] data) {
        int seqNum = (int) data[2];
        Object val = data[3];
        markAsConsensus(seqNum, val,false);
    }

    // =======================================
    // UTILS
    // =======================================

    private boolean waitForMajority(Map<Integer, Integer> map, int seqNum) {
        int majority = (int) Math.floor(gcl.getAllMembers().length / 2.0) + 1;
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < 3000) { // 3-second timeout
            if (map.getOrDefault(seqNum, 0) >= majority)
                return true;
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        return false;
    }

    private synchronized long generateProposalNumber() {
        long processHash = Math.abs(myProcess.hashCode() % 10000);
        return (proposalCounter++ * 100000) + processHash;
    }

    private synchronized int getNextSequenceNumber() {
        return nextSequenceNumber++;
    }
}
