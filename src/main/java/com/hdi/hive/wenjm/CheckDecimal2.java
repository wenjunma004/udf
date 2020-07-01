package com.hdi.hive.wenjm;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;

public class CheckDecimal2 extends GenericUDTF {
    private PrimitiveObjectInspector argumentOI = null;
    //Defining input argument as decimal.
    @Override
    public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        if (args.length != 1) {
            throw new UDFArgumentException("CheckDecimal2() takes exactly one argument");
        }

        if (args[0].getCategory() != ObjectInspector.Category.PRIMITIVE
                && ((PrimitiveObjectInspector) args[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.DECIMAL) {
            throw new UDFArgumentException("NameParserGenericUDTF() takes a decimal as a parameter");
        }

        argumentOI = (PrimitiveObjectInspector) args[0];

        // output
        List<String> fieldNames = new ArrayList<String>(2);
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(2);
        fieldNames.add("Original");
        fieldNames.add("Status");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }


    @Override
    public void process(Object[] arguments) throws HiveException {
        ArrayList<Object[]> results = new ArrayList<Object[]>();
        Object valObject = arguments[0];
        if (valObject == null) {
            results.add(new Object[] {"NULL", "OK" });
        }else{
            HiveDecimalObjectInspector decimalOI =
                    (HiveDecimalObjectInspector) argumentOI;
            HiveDecimalWritable val = decimalOI.getPrimitiveWritableObject(valObject);

            // NPE check logic start

            HiveDecimal decimalTypeInfo = decimalOI.getPrimitiveJavaObject(valObject);
            DecimalTypeInfo decTypeInfo = (DecimalTypeInfo)decimalOI.getTypeInfo();
            try {
//          int prec = decTypeInfo.precision();
                int scale = decTypeInfo.scale();
                byte[] decimalBytes = decimalTypeInfo.bigIntegerBytesScaled(scale);

                results.add(new Object[] {valObject.toString(), "OK" });
            }catch (NullPointerException npe){
                npe.printStackTrace();
                results.add(new Object[] {valObject.toString(), "FAIL" });

            }

        }

        // NPE check logic end
        Iterator<Object[]> it = results.iterator();

        while (it.hasNext()){
            Object[] r = it.next();
            forward(r);
        }
    }

    @Override
    public void close() throws HiveException {
        // do nothing
    }
}
