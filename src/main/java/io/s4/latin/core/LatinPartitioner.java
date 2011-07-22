package io.s4.latin.core;

import io.s4.dispatcher.partitioner.CompoundKeyInfo;
import io.s4.dispatcher.partitioner.Hasher;
import io.s4.dispatcher.partitioner.KeyInfo;
import io.s4.dispatcher.partitioner.Partitioner;
import io.s4.dispatcher.partitioner.VariableKeyPartitioner;
import io.s4.latin.pojo.StreamRow;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

public class LatinPartitioner implements Partitioner, VariableKeyPartitioner {
    private List<List<String>> keyNameTuple = new ArrayList<List<String>>();
    private boolean debug = false;
    private Hasher hasher;
    private Set<String> streamNameSet;
    private String delimiter = ":";
    private boolean fastPath = false;

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public void setHashKey(String[] simpleKeyStrings) {
        for (String simpleKeyAsString : simpleKeyStrings) {
            List<String> keyNameElements = new ArrayList<String>();
            StringTokenizer st = new StringTokenizer(simpleKeyAsString, "/");
            while (st.hasMoreTokens()) {
                keyNameElements.add(st.nextToken());
            }
            keyNameTuple.add(keyNameElements);
        }
    }

    public void setStreamNames(String[] streamNames) {
        streamNameSet = new HashSet<String>(streamNames.length);
        for (String eventType : streamNames) {
            streamNameSet.add(eventType);
        }
    }

    public void setHasher(Hasher hasher) {
        this.hasher = hasher;
    }

    public void setDebug(boolean debug) {
        this.debug = debug;
    }


    public List<CompoundKeyInfo> partition(String streamName, Object event,
                                           int partitionCount) {
    	return partition(streamName, keyNameTuple, event, partitionCount);
    }

    public List<CompoundKeyInfo> partition(String streamName,
                                           List<List<String>> compoundKeyNames,
                                           Object event, int partitionCount) {

    	System.err.println("Partition StreamRow! :" + event);
        if (streamName != null && streamNameSet != null
                && !streamNameSet.contains(streamName) ) {
            return null;
        }

        if (event instanceof StreamRow) {
        	System.err.println("Event not of type StreamRow! :" + event);
        	return null;
        }


        if (compoundKeyNames == null) {
            // if compoundKeyNames is null, then assign to a random partition.
            return partitionRandom(partitionCount);
        }

        StreamRow row = (StreamRow) event;
        List<CompoundKeyInfo> partitionInfoList = new ArrayList<CompoundKeyInfo>();

        // fast path for single top-level key
        if (fastPath
                || (compoundKeyNames.size() == 1 && compoundKeyNames.get(0)
                                                                    .size() == 1)) {
            String simpleKeyName = compoundKeyNames.get(0).get(0);
            if (debug) {
                System.out.println("Using fast path! SimpleKeyname : " + simpleKeyName);
            }
            fastPath = true;
            KeyInfo keyInfo = new KeyInfo();
            
            Object value = event;
            try {
                value = row.get(simpleKeyName);
            } catch (Exception e) {
                if (debug) {
                    e.printStackTrace();
                }
            }

            if (value == null) {
                if (debug) {
                    System.out.println("Fast path: Null value encountered");
                }
                return null;
            }
            keyInfo.addElementToPath(simpleKeyName);
            String stringValue = String.valueOf(value);
            keyInfo.setValue(stringValue);
            CompoundKeyInfo partitionInfo = new CompoundKeyInfo();
            partitionInfo.addKeyInfo(keyInfo);
            int partitionId = (int) (hasher.hash(stringValue) % partitionCount);
            partitionInfo.setPartitionId(partitionId);
            partitionInfo.setCompoundValue(stringValue);
            partitionInfoList.add(partitionInfo);
            if (debug) {
                System.out.printf("Value %s, partition id %d\n",
                                  stringValue,
                                  partitionInfo.getPartitionId());
            }
            return partitionInfoList;
        }

        List<List<KeyInfo>> valueLists = new ArrayList<List<KeyInfo>>();
        int maxSize = 0;

        for (List<String> simpleKeyPath : compoundKeyNames) {
            List<KeyInfo> keyInfoList = new ArrayList<KeyInfo>();
            KeyInfo keyInfo = new KeyInfo();
            keyInfoList = getKeyValues(event,
                                       simpleKeyPath,
                                       0,
                                       keyInfoList,
                                       keyInfo);
            if (keyInfoList == null || keyInfoList.size() == 0) {
                if (debug) {
                    System.out.println("Null value encountered");
                }
                return null; // do no partitioning if any simple key's value
                             // resolves to null
            }
            valueLists.add(keyInfoList);
            maxSize = Math.max(maxSize, keyInfoList.size());

            if (debug) {
                printKeyInfoList(keyInfoList);
            }
        }

        for (int i = 0; i < maxSize; i++) {
            String compoundValue = "";
            CompoundKeyInfo partitionInfo = new CompoundKeyInfo();
            for (List<KeyInfo> keyInfoList : valueLists) {
                if (i < keyInfoList.size()) {
                    compoundValue += (compoundValue.length() > 0 ? delimiter
                            : "") + keyInfoList.get(i).getValue();
                    partitionInfo.addKeyInfo(keyInfoList.get(i));
                } else {
                    compoundValue += (compoundValue.length() > 0 ? delimiter
                            : "")
                            + keyInfoList.get(keyInfoList.size() - 1)
                                         .getValue();
                    partitionInfo.addKeyInfo(keyInfoList.get(keyInfoList.size() - 1));
                }
            }

            // get the partition id
            int partitionId = (int) (hasher.hash(compoundValue) % partitionCount);
            partitionInfo.setPartitionId(partitionId);
            partitionInfo.setCompoundValue(compoundValue);
            partitionInfoList.add(partitionInfo);
            if (debug) {
                System.out.printf("Value %s, partition id %d\n",
                                  compoundValue,
                                  partitionInfo.getPartitionId());
            }
        }

        return partitionInfoList;
    }

    // Assign to random partition
    private List<CompoundKeyInfo> partitionRandom(int partitionCount) {
        CompoundKeyInfo partitionInfo = new CompoundKeyInfo();

        // choose a random int from [0, partitionCount-1]
        int partitionId = (int) Math.min(partitionCount - 1,
                                         Math.floor(Math.random()
                                                 * partitionCount));

        partitionInfo.setPartitionId(partitionId);
        List<CompoundKeyInfo> partitionInfoList = new ArrayList<CompoundKeyInfo>();
        partitionInfoList.add(partitionInfo);

        return partitionInfoList;
    }

    private void printKeyInfoList(List<KeyInfo> keyInfoList) {
        for (KeyInfo aKeyInfo : keyInfoList) {
            System.out.printf("Path: %s; full path %s; value %s\n",
                              aKeyInfo.toString(),
                              aKeyInfo.toString(true),
                              aKeyInfo.getValue());
        }
    }

    private List<KeyInfo> getKeyValues(Object record,
                                       List<String> keyNameElements,
                                       int elementIndex,
                                       List<KeyInfo> keyInfoList,
                                       KeyInfo keyInfo) {
        String keyElement = keyNameElements.get(elementIndex);

        keyInfo.addElementToPath(keyElement);

        Object value = null;
        try {
            value = ((StreamRow) record).get(keyElement);
        } catch (Exception e) {
            if (debug) {
                System.out.println("key element is " + keyElement);
                e.printStackTrace();
            }
        }

        if (value == null) {
            return null; // return a null KeyInfo list if we hit a null value
        }

        keyInfo.setValue(String.valueOf(value));
        keyInfoList.add(keyInfo);
        
        return keyInfoList;
    }
}
