package org.apache.nifi.atlas;

import org.apache.atlas.hook.AtlasHook;
import org.apache.atlas.notification.hook.HookNotification;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.nifi.atlas.provenance.DataSetRefs;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.nifi.atlas.NiFiTypes.ATTR_DESCRIPTION;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_INPUTS;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_NIFI_FLOW;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_OUTPUTS;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_URL;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_FLOW;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_FLOW_PATH;

public class ITAtlasHookExample extends AtlasHook {

    @Override
    protected String getNumberOfRetriesPropertyKey() {
        return "atlas.hook.nifi.numRetries";
    }

    @Test
    public void testNiFiFlowPath() throws Exception {

        final NiFIAtlasHook hook = new NiFIAtlasHook();

        final DataSetRefs refs = new DataSetRefs("03eb0e87-015e-1000-0000-00007e0d56ea");
        final Referenceable topic = new Referenceable("kafka_topic");
        topic.set(ATTR_NAME, "notification");
        topic.set("topic", "notification");
        topic.set(ATTR_QUALIFIED_NAME, "notification@HDPF");
        topic.set(ATTR_DESCRIPTION, "Description");
        topic.set("uri", "0.hdpf.aws.mine");
        refs.addInput(topic);

        final Referenceable flowRef = new Referenceable(TYPE_NIFI_FLOW);
        flowRef.set(ATTR_NAME, "NiFi Flow");
        flowRef.set(ATTR_QUALIFIED_NAME, "7c84501d-d10c-407c-b9f3-1d80e38fe36a ");
        flowRef.set(ATTR_URL, "http://0.hdpf.aws.mine:9990/");

        final Referenceable flowPathRef = new Referenceable(TYPE_NIFI_FLOW_PATH);
        flowPathRef.set(ATTR_NAME, "ConsumeKafka");
        flowPathRef.set(ATTR_QUALIFIED_NAME, "03eb0e87-015e-1000-0000-00007e0d56ea");
        flowPathRef.set(ATTR_NIFI_FLOW, flowRef);
        flowPathRef.set(ATTR_URL, "http://0.hdpf.aws.mine:9990/");


        hook.addDataSetRefs(refs, flowPathRef);
        hook.commitMessages();
    }

    public void sendMessage() throws Exception {
        final List<HookNotification.HookNotificationMessage> messages = new ArrayList<>();
        final Referenceable flow = new Referenceable(TYPE_NIFI_FLOW);
        flow.set(ATTR_NAME, "ut");
        flow.set(ATTR_QUALIFIED_NAME, "ut");
        flow.set(ATTR_DESCRIPTION, "Description");
        flow.set(ATTR_URL, "http://localhost:8080/nifi");

        final Referenceable path1 = new Referenceable(TYPE_NIFI_FLOW_PATH);
        path1.set(ATTR_NAME, "path1");
        path1.set(ATTR_QUALIFIED_NAME, "path1");
        path1.set(ATTR_DESCRIPTION, "Description");
        path1.set(ATTR_NIFI_FLOW, flow);

        final Referenceable path2 = new Referenceable(TYPE_NIFI_FLOW_PATH);
        path2.set(ATTR_NAME, "path2");
        path2.set(ATTR_QUALIFIED_NAME, "path2");
        path2.set(ATTR_DESCRIPTION, "Description");
        path2.set(ATTR_NIFI_FLOW, flow);

        final Referenceable path3 = new Referenceable(TYPE_NIFI_FLOW_PATH);
        path3.set(ATTR_NAME, "path3");
        path3.set(ATTR_QUALIFIED_NAME, "path3");
        path3.set(ATTR_DESCRIPTION, "Description");
        path3.set(ATTR_NIFI_FLOW, flow);

        final Referenceable path4 = new Referenceable(TYPE_NIFI_FLOW_PATH);
        path4.set(ATTR_NAME, "path4");
        path4.set(ATTR_QUALIFIED_NAME, "path4");
        path4.set(ATTR_DESCRIPTION, "Description");
        path4.set(ATTR_NIFI_FLOW, flow);

        final Referenceable path5 = new Referenceable(TYPE_NIFI_FLOW_PATH);
        path5.set(ATTR_NAME, "path5");
        path5.set(ATTR_QUALIFIED_NAME, "path5");
        path5.set(ATTR_DESCRIPTION, "Description");
        path5.set(ATTR_NIFI_FLOW, flow);

        final ArrayList<Object> path1Outputs = new ArrayList<>();
        path1Outputs.add(new Referenceable(path2));
        path1.set(ATTR_OUTPUTS, path1Outputs);

        final ArrayList<Object> path2Inputs = new ArrayList<>();
        path2Inputs.add(new Referenceable(path1));
        path2.set(ATTR_INPUTS, path2Inputs);

        final ArrayList<Object> path2Outputs = new ArrayList<>();
        path2Outputs.add(new Referenceable(path3));
        path2Outputs.add(new Referenceable(path4));
        path2.set(ATTR_OUTPUTS, path2Outputs);

        final ArrayList<Object> path3Inputs = new ArrayList<>();
        path3Inputs.add(new Referenceable(path2));
        path3.set(ATTR_INPUTS, path3Inputs);

        final ArrayList<Object> path3Outputs = new ArrayList<>();
        path3Outputs.add(new Referenceable(path4));
        path3.set(ATTR_OUTPUTS, path3Outputs);

        final ArrayList<Object> path4Inputs = new ArrayList<>();
        path4Inputs.add(new Referenceable(path3));
        path4.set(ATTR_INPUTS, path4Inputs);

        final ArrayList<Object> path4Outputs = new ArrayList<>();
        path4Outputs.add(new Referenceable(path5));
        path4.set(ATTR_OUTPUTS, path4Outputs);

        final ArrayList<Object> path5Inputs = new ArrayList<>();
        path5Inputs.add(new Referenceable(path4));
        path5.set(ATTR_INPUTS, path5Inputs);


        final HookNotification.EntityCreateRequest message = new HookNotification.EntityCreateRequest("nifi", path2);
        messages.add(message);
        notifyEntities(messages);
    }

    public void createLineageFromKafkaTopic() throws Exception {
        final List<HookNotification.HookNotificationMessage> messages = new ArrayList<>();
        final Referenceable topic = new Referenceable("kafka_topic");
        topic.set(ATTR_NAME, "trucking-data");
        topic.set("topic", "trucking-data");
        topic.set(ATTR_QUALIFIED_NAME, "trucking-data@HDPF");
        topic.set(ATTR_DESCRIPTION, "Description");
        topic.set("uri", "0.hdpf.aws.mine");
        final HookNotification.EntityCreateRequest createTopic = new HookNotification.EntityCreateRequest("nifi", topic);


        final Referenceable path1 = new Referenceable(TYPE_NIFI_FLOW_PATH);
        path1.set(ATTR_QUALIFIED_NAME, "path1");
        final ArrayList<Object> path1Inputs = new ArrayList<>();
        path1Inputs.add(new Referenceable(topic));
        path1.set(ATTR_INPUTS, path1Inputs);

        messages.add(createTopic);
        messages.add(new HookNotification.EntityPartialUpdateRequest("nifi", TYPE_NIFI_FLOW_PATH, ATTR_QUALIFIED_NAME, "path1", path1));
        notifyEntities(messages);

    }

    public void createLineageToKafkaTopic() throws Exception {
        final List<HookNotification.HookNotificationMessage> messages = new ArrayList<>();
        final Referenceable topic = new Referenceable("kafka_topic");
        topic.set(ATTR_NAME, "notification");
        topic.set("topic", "notification");
        topic.set(ATTR_QUALIFIED_NAME, "notification@HDPF");
        topic.set(ATTR_DESCRIPTION, "Description");
        topic.set("uri", "0.hdpf.aws.mine");
        final HookNotification.EntityCreateRequest createTopic = new HookNotification.EntityCreateRequest("nifi", topic);


        final Referenceable path5 = new Referenceable(TYPE_NIFI_FLOW_PATH);
        path5.set(ATTR_QUALIFIED_NAME, "path5");
        final ArrayList<Object> path5Outputs = new ArrayList<>();
        path5Outputs.add(new Referenceable(topic));
        path5.set(ATTR_OUTPUTS, path5Outputs);

        messages.add(createTopic);
        messages.add(new HookNotification.EntityPartialUpdateRequest("nifi", TYPE_NIFI_FLOW_PATH, ATTR_QUALIFIED_NAME, "path5", path5));
        notifyEntities(messages);

    }

}
