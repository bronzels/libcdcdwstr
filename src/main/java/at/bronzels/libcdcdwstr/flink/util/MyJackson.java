package at.bronzels.libcdcdwstr.flink.util;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.JsonNodeType;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class MyJackson {
    static private final Logger LOG = LoggerFactory.getLogger(MyJackson.class);

    static public List<Object> getDynamicFieldsNameObjTupleList(JsonNode node, List<String> fieldNameList) {
        //LOG.trace("node:{}, fieldIndexList:{}", node, fieldNameList);
        return fieldNameList.stream()
                .map(name -> new Tuple2<String, Object>(name, node.get(name)))
                .collect(Collectors.toList())
                ;
    }

    static public boolean isFieldNullType(JsonNode input, String fieldName) {
        return input.get(fieldName).getNodeType().equals(JsonNodeType.NULL);
    }

    static public List<Double> getDoubleList(JsonNode record, List<String> fieldList) {
        return fieldList.stream()
                .map(field -> {
                    JsonNode node = record.get(field);
                    return (node != null && node.isDouble()) ? node.asDouble() : null;
                })
                .collect(Collectors.toList());
    }

    static public JsonNode getProjected(JsonNode record, List<String> fieldList, boolean toProjectOrRemove) {
        ObjectNode ret = record.deepCopy();
        Iterator<String> iterator = record.fieldNames();
        while (iterator.hasNext()) {
            String field = iterator.next();
            if(toProjectOrRemove) {
                if (!fieldList.contains(field))
                    ret.remove(field);
            } else {
                if (fieldList.contains(field))
                    ret.remove(field);
            }
        }
        return ret;
    }

    static public JsonNode getProjected(JsonNode record, List<String> fieldList) {
        return getProjected(record, fieldList, true);
    }

    static public JsonNode getRemoved(JsonNode record, List<String> fieldList) {
        return getProjected(record, fieldList, false);
    }

    static public void assertJsonNodeType(JsonNode jsonNode, JsonNodeType jsonNodeType) {
        if(!(jsonNode.getNodeType().equals(jsonNodeType))) throw new RuntimeException(String.format("jsonNode is not in jsonNodeType:%s, :\n", jsonNodeType, jsonNode));
    }

    static public JsonNode getMerged(JsonNode... nodes) {
        int length = nodes.length;
        if(length < 2) {
            throw new RuntimeException(String.format("wrong nodes less than 2:%s", getString(nodes[0])));
        }
        JsonNode copiedNode = nodes[0].deepCopy();
        assertJsonNodeType(copiedNode, JsonNodeType.OBJECT);
        ObjectNode ret = (ObjectNode) copiedNode;
        for (int i = 1; i < length; i++) {
            JsonNode node = nodes[i];
            assertJsonNodeType(node, JsonNodeType.OBJECT);
            ObjectNode toAdd = (ObjectNode) node;
            Iterator<String> iterator = node.fieldNames();
            while (iterator.hasNext()) {
                String field = iterator.next();
                ret.put(field, toAdd.get(field));
            }
        }
        return ret;
    }

    /*
    static public Map<String, Object> getMap(ObjectMapper mapper, JsonNode jsonNode) {
        return //mapper.convertValue(jsonNode, Map.class);
                jsonNode == null ? null : mapper.convertValue(jsonNode, new TypeReference<Map<String, Object>>(){});
    }
     */
    static public Map<String, Object> getMap(JsonNode jsonNode) {
        if (jsonNode == null) return null;
        Map<String, Object> ret = new HashMap<>();
        Iterator<String> iterator = jsonNode.fieldNames();
        while (iterator.hasNext()) {
            String field = iterator.next();
            ret.put(field, getElementAsObj(jsonNode.get(field)));
        }
        return ret;
    }

    /*
    static public Object getByType(JsonNode jsonNode, String fieldName, TargetDataType fieldType) {
        assertJsonNodeType(jsonNode, JsonNodeType.OBJECT);
        Object ret = null;
        JsonNode fieldNode = jsonNode.get(fieldName);
        if(fieldNode != null) {
            switch (fieldType) {
                case INT:
                    ret = fieldNode.asInt();
                    break;
                case LONG:
                    ret = fieldNode.asLong();
                    break;
                case DOUBLE:
                    ret = fieldNode.asDouble();
                    break;
                case STRING:
                    ret = fieldNode.asText();
                    break;
                default:
                    com.fm.data.libcommon.util.Debug.assertDirect(String.format("not supported TargetDataType:%s", fieldType));
            }
        }
        return ret;
    }
     */

    //not tested yet
    static public Object getElementAsObj(JsonNode elementJsonNode) {
        Object ret = null;
        if (elementJsonNode.isInt()) {
            ret = elementJsonNode.asInt();
        } else if (elementJsonNode.isLong()) {
            ret = elementJsonNode.asLong();
        } else if (elementJsonNode.isDouble()) {
            ret = elementJsonNode.asDouble();
        } else if (elementJsonNode.isTextual()) {
            ret = elementJsonNode.asText();
        } else if (elementJsonNode.isBoolean()) {
            ret = elementJsonNode.asBoolean();
        } else if (elementJsonNode.isNull()) {
            ret = null;
        } else {
            throw new RuntimeException(String.format("not supported elementJsonNode:%s", getString(elementJsonNode)));
        }
        return ret;
    }

    static public Object get(JsonNode jsonNode, String field) {
        JsonNode elementJsonNode = jsonNode.get(field);
        if (elementJsonNode == null) return null;
        if (elementJsonNode.isNull()) return null;
        Object ret = null;
        if (elementJsonNode.isInt()) {
            ret = elementJsonNode.asInt();
        } else if (elementJsonNode.isLong()) {
            ret = elementJsonNode.asLong();
        } else if (elementJsonNode.isDouble()) {
            ret = elementJsonNode.asDouble();
        } else if (elementJsonNode.isTextual()) {
            ret = elementJsonNode.asText();
        } else if (elementJsonNode.isBoolean()) {
            ret = elementJsonNode.asBoolean();
        } else
            throw new RuntimeException(String.format("not supported elementJsonNode:%s", getString(elementJsonNode)));
        return ret;
    }

    static public Long getAsLong(JsonNode jsonNode, String field) {
        JsonNode elementJsonNode = jsonNode.get(field);
        if (elementJsonNode == null) return null;
        if (elementJsonNode.isNull()) return null;
        Long ret = null;
        if (elementJsonNode.isInt()) {
            ret = (long)elementJsonNode.asInt();
        } else if (elementJsonNode.isLong()) {
            ret = elementJsonNode.asLong();
        } else
            throw new RuntimeException(String.format("not supported elementJsonNode:%s", getString(elementJsonNode)));
        return ret;
    }

    static public String getAsString(JsonNode jsonNode, String field) {
        JsonNode elementJsonNode = jsonNode.get(field);
        if (elementJsonNode == null) return null;
        if (elementJsonNode.isNull()) return null;
        String ret = null;
        if (elementJsonNode.isTextual()) {
            ret = elementJsonNode.asText();
        } else {
            ret = elementJsonNode.toString();
        }

        return ret;
    }

    static public JsonNode getObjNode(JsonNode jsonNode, String field) {
        JsonNode ret = jsonNode.get(field);
        if (ret == null) return null;
        if (!ret.isObject()) return null;
        return ret;
    }

    static public Object get(JsonNode jsonNode, String field, boolean toLowerCased) {
        String lowerCasedField;
        if (toLowerCased)
            lowerCasedField = field.toLowerCase();
        else lowerCasedField = field;
        return get(jsonNode, lowerCasedField);
    }

    static public Long getAsLong(JsonNode jsonNode, String field, boolean toLowerCased) {
        String lowerCasedField;
        if (toLowerCased)
            lowerCasedField = field.toLowerCase();
        else lowerCasedField = field;
        return getAsLong(jsonNode, lowerCasedField);
    }

    static public String getLVDName(String name, boolean toLowerCase) {
        return toLowerCase ? name.toLowerCase() : name;
    }

    //not tested yet
    static public ArrayList<Object> getObjListFromObjNode(JsonNode jsonNode) {
        ArrayList<Object> ret = new ArrayList<>();
        Iterator<JsonNode> iterator = jsonNode.elements();
        while (iterator.hasNext()) {
            JsonNode node = iterator.next();
            ret.add(getElementAsObj(node));
        }
        return ret;
    }

    static public Map<String, Object> getProjectedMap(JsonNode jsonNode, Collection<String> fields2Project) {
        if (jsonNode == null) return null;
        Map<String, Object> ret = new HashMap<>();
        Iterator<String> iterator = jsonNode.fieldNames();
        while (iterator.hasNext()) {
            String field = iterator.next();
            if (fields2Project == null || fields2Project.contains(field))
                ret.put(field, getElementAsObj(jsonNode.get(field)));
        }
        return ret;
    }

    static public String getString(JsonNode node) {
        ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
        String ret = null;
        try{
            ret = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(node);
        }catch (JsonProcessingException e){
            e.printStackTrace();
        }
        return ret;
    }

    static public boolean isExistedValueNode(JsonNode node, String keyName) {
        return node.has(keyName) && node.get(keyName).isValueNode();
    }

    static public boolean isExistedTypeAlliedNode(JsonNode node, String keyName, JsonNodeType type) {
        return node.has(keyName) && node.get(keyName).getNodeType().equals(type);
    }

    static public ArrayNode getArrayNode(JsonNode node, String keyName) {
        ArrayNode ret = null;
        if(node.has(keyName)) {
            JsonNode arrNode = node.get(keyName);
            if(arrNode.isArray() && arrNode.size() > 0)
                ret = (ArrayNode)arrNode;
        }
        return ret;
    }

    static public ObjectNode getMapAdded(ObjectNode node, Map<String, Object> map2Add, Map<String, TypeInformation> typeMap2Add) {
        for(Map.Entry<String, Object> entry: map2Add.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            TypeInformation typeInfo = typeMap2Add.get(key);
            if(typeInfo.equals(BasicTypeInfo.INT_TYPE_INFO))
                node.put(key, (Integer)value);
            else if(typeInfo.equals(BasicTypeInfo.LONG_TYPE_INFO))
                node.put(key, (Long)value);
            else if(typeInfo.equals(BasicTypeInfo.DOUBLE_TYPE_INFO))
                node.put(key, (Double)value);
            else if(typeInfo.equals(BasicTypeInfo.STRING_TYPE_INFO))
                node.put(key, (String)value);
            else if(typeInfo.equals(BasicTypeInfo.BOOLEAN_TYPE_INFO))
                node.put(key, (Boolean) value);
            else
                throw new RuntimeException("暂不支持Int/Long/Double/String/Boolean/Array<String>以外类型");
        }
        return node;
    }

}
