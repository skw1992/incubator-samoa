package org.apache.samoa.topology.impl.gearpump;

/*
 * #%L
 * SAMOA
 * %%
 * Copyright (C) 2014 - 2015 Apache Software Foundation
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import org.apache.gearpump.partitioner.ShufflePartitioner;
import org.apache.gearpump.streaming.Processor;
import org.apache.gearpump.util.Graph;
import org.apache.samoa.topology.AbstractTopology;
import org.apache.samoa.topology.IProcessingItem;
import org.apache.samoa.topology.Stream;
import org.apache.samoa.utils.PartitioningScheme;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class Topology extends AbstractTopology {

    private Graph graph;
    private Map<IProcessingItem,Processor> piToProcessor;

    public Topology(String name) {
        super(name);
        graph = Graph.empty();
        piToProcessor = new HashMap<IProcessingItem,Processor>();
    }

    public Graph getGraph() {
        buildGraph();
        return graph;
    }

    private void buildGraph() {
        Set<IProcessingItem> processingItems = getProcessingItems();
        for (IProcessingItem procItem : processingItems) {
            TopologyNode gearpumpNode = (TopologyNode) procItem;
            Processor gearpumpProcessor = gearpumpNode.createGearpumpProcessor();
            piToProcessor.put(procItem, gearpumpProcessor);
            graph.addVertex(gearpumpProcessor);
        }

        Set<Stream> streams = getStreams();
        for (Stream stream : streams) {
            GearpumpStream gearpumpStream = (GearpumpStream) stream;
            IProcessingItem sourcePi = gearpumpStream.getSourceProcessingItem();
            IProcessingItem targetPi = gearpumpStream.getTargetPi();
            Processor sourceProcessor = piToProcessor.get(sourcePi);
            Processor targetProcessor = piToProcessor.get(targetPi);
            PartitioningScheme scheme = gearpumpStream.getScheme();
            ShufflePartitioner shufflePartitioner;
            switch (scheme) {
                case SHUFFLE:
                    shufflePartitioner = new ShufflePartitioner();
                    graph.addEdge(sourceProcessor, shufflePartitioner, targetProcessor);
                    break;
                case GROUP_BY_KEY:
                    shufflePartitioner = new ShufflePartitioner();
                    graph.addEdge(sourceProcessor, shufflePartitioner, targetProcessor);
                    break;
                case BROADCAST:
                    shufflePartitioner = new ShufflePartitioner();
                    graph.addEdge(sourceProcessor, shufflePartitioner, targetProcessor);
                    break;
            }
        }
    }
}
