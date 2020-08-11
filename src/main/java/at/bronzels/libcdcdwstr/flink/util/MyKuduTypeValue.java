package at.bronzels.libcdcdwstr.flink.util;

import at.bronzels.libcdcdw.bean.MyLogContext;
import at.bronzels.libcdcdw.util.MyDateTime;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.kudu.Type;

import org.apache.logging.log4j.LogManager;

import java.util.*;

public class MyKuduTypeValue {
    private static org.apache.logging.log4j.Logger lo4j2LOG = LogManager.getLogger(MyKuduTypeValue.class);

    static public io.vavr.Tuple2<Type, Object> getTypeValueByJsonNode(JsonNode node) {
        Type kuduType = null;
        Object value = null;
        if (node.isDouble() || node.isFloat()) {
            kuduType = Type.DOUBLE;
            value = node.asDouble();
        } else if (node.isLong()) {
            kuduType = Type.INT64;
            value = node.asLong();
        } else if (node.isShort() || node.isInt()) {
            kuduType = Type.INT64;
            value = Integer.valueOf(node.asInt()).longValue();
        } else if (node.isTextual()) {
            kuduType = Type.STRING;
            value = node.asText();
        } else if (node.isBoolean()) {
            kuduType = Type.BOOL;
            value = node.asBoolean();
        } else if (node.isArray() || node.isObject()) {
            kuduType = Type.STRING;
            value = node.toString();
        }

        if (kuduType != null && value != null)
            return new io.vavr.Tuple2<Type, Object>(kuduType, value);
        else
            return null;
    }

    static public Object getValueByJsonNodeType(JsonNode node, Type inputType) throws NumberFormatException {
        Object ret = null;
        if(node == null)
            return null;
        try {
            if (node.isDouble() || node.isFloat()) {
                ret = node.asDouble();
                ret = at.bronzels.libcdcdw.util.MyKuduTypeValue.getDoubleConverted(ret, inputType);
            } else if (node.isLong() || node.isShort() || node.isInt()) {
                if (node.isLong())
                    ret = node.asLong();
                else
                    ret = Integer.valueOf(node.asInt()).longValue();
                ret = at.bronzels.libcdcdw.util.MyKuduTypeValue.getLongConverted(ret, inputType);
            } else if (node.isTextual()) {
                ret = node.asText();
                ret = at.bronzels.libcdcdw.util.MyKuduTypeValue.getStringConverted(ret, inputType);
            } else if (node.isBoolean()) {
                ret = node.asBoolean();
                ret = at.bronzels.libcdcdw.util.MyKuduTypeValue.getBoolConverted(ret, inputType);
            } else if (node.isArray() || node.isObject()) {
                ret = node.toString();
                if (!inputType.equals(Type.STRING))
                    ret = null;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ret;
    }

    static public Tuple2<Map<String, Type>, Map<String, Object>> getCol2AddAndValueMapTuple(Map<String, Type> colName2TypeMap, JsonNode node, boolean isSrcFieldNameWTUpperCase, MyLogContext logContext) {
        Map<String, Type> col2AddMap = getJsonCol2Ad(colName2TypeMap, node, isSrcFieldNameWTUpperCase);
        Map<String, Object> valueMap = new HashMap<>();
        ObjectNode objectNode = (ObjectNode) node;
        Iterator<String> stringIterator = objectNode.fieldNames();
        while (stringIterator.hasNext()) {
            String key = stringIterator.next();
            JsonNode value = node.get(key);
            String realKey4SinkDestination = isSrcFieldNameWTUpperCase ? key.toLowerCase() : key;
            Type inputType = colName2TypeMap.get(realKey4SinkDestination) == null ? col2AddMap.get(realKey4SinkDestination) : colName2TypeMap.get(realKey4SinkDestination);
            Object convertedValue = null;
            io.vavr.Tuple2<Type, Object> typeValueTuple2 = getTypeValueByJsonNode(value);

            if (typeValueTuple2 != null && typeValueTuple2._1 != null){
                if (typeValueTuple2._1 == inputType || (typeValueTuple2._1 == Type.INT64 && inputType == Type.UNIXTIME_MICROS)){
                    convertedValue = getValueByJsonNodeType(value, inputType);
                }else
                    MyLogContextMsg.logNodeError(node, logContext, "value data type error detected, current value is not equal kudu exists column type");
            }else
                MyLogContextMsg.logNodeError(node, logContext, "value data type error detected, current value is not equal kudu exists column type");
            if (convertedValue == null) {
                MyLogContextMsg.logNodeError(node, logContext, "unsupported jsonNode/kuduTableFieldType detected, or parse error");
            } else {
                valueMap.put(realKey4SinkDestination, convertedValue);
            }
        }
        return new Tuple2<Map<String, Type>, Map<String, Object>>(col2AddMap, valueMap);
    }

    public static Map<String, Type> getJsonCol2Ad(Map<String, Type> colName2TypeMap, JsonNode node,  boolean isSrcFieldNameWTUpperCase) {
        Set<String> colNameSet = colName2TypeMap.keySet();
        Map<String, Type> col2AddMap = new HashMap<>();
        ObjectNode objectNode = (ObjectNode) node;
        Iterator<String> stringIterator = objectNode.fieldNames();
        while (stringIterator.hasNext()) {
            String key = stringIterator.next();
            JsonNode value = node.get(key);
            String realKey4SinkDestination = isSrcFieldNameWTUpperCase ? key.toLowerCase() : key;
            if (!colNameSet.contains(realKey4SinkDestination)) {
                io.vavr.Tuple2<Type, Object> tuple = getTypeValueByJsonNode(value);
                if (tuple != null) {
                    col2AddMap.put(realKey4SinkDestination, tuple._1);
                }
            }
        }
        return col2AddMap;
    }


    static public Long getLong(JsonNode node, String keyName) {
        Object ret = MyKuduTypeValue.getValueByJsonNodeType(node.get(keyName), Type.INT32);
        if (ret != null)
            return (Long) ret;
        else
            return null;
    }

    static public Double getDouble(JsonNode node, String keyName) {
        Object ret = MyKuduTypeValue.getValueByJsonNodeType(node.get(keyName), Type.DOUBLE);
        if (ret != null)
            return (Double) ret;
        else
            return null;
    }

    static public String getString(JsonNode node, String keyName) {
        Object ret = MyKuduTypeValue.getValueByJsonNodeType(node.get(keyName), Type.STRING);
        if (ret != null)
            return (String) ret;
        else
            return null;
    }

    static public Long getTimestamp(JsonNode node, String keyName) {
        Object ret = MyKuduTypeValue.getValueByJsonNodeType(node.get(keyName), Type.UNIXTIME_MICROS);
        if (ret != null)
            return (Long) ret;
        else
            return null;
    }

    static public Boolean getBool(JsonNode node, String keyName) {
        Object ret = MyKuduTypeValue.getValueByJsonNodeType(node.get(keyName), Type.BOOL);
        if (ret != null)
            return (Boolean) ret;
        else
            return null;
    }

}
