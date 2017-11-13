package extend.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;

/**
 * GenericUDFArrayContainsAll.
 *
 */
@Description(name = "array_contains_all",
    value="_FUNC_(array, array) - Returns TRUE if the array contains value.",
    extended="Example:\n"
      + "  > SELECT _FUNC_(array(1, 2, 3), array(2, 4)) FROM src LIMIT 1;\n"
      + " false")
public class GenericUDFArrayContainsAll extends GenericUDF {

  private static final int ARRAY_IDX = 0;
  private static final int VALUE_IDX = 1;
  private static final int ARG_COUNT = 2; // Number of arguments to this UDF
  private static final String FUNC_NAME = "ARRAY_CONTAINS_ALL"; // External Name

  private transient ListObjectInspector valueOI;
  private transient ListObjectInspector arrayOI;
  private transient ObjectInspector arrayElementOI;
  private transient ObjectInspector valueElementOI;
  private BooleanWritable result;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentException {

    // Check if two arguments were passed
    if (arguments.length != ARG_COUNT) {
      throw new UDFArgumentException(
          "The function " + FUNC_NAME + " accepts "
              + ARG_COUNT + " arguments.");
    }

    // Check if ARRAY_IDX argument is of category LIST
    if (!arguments[ARRAY_IDX].getCategory().equals(Category.LIST)) {
      throw new UDFArgumentTypeException(ARRAY_IDX,
          "\"" + org.apache.hadoop.hive.serde.serdeConstants.LIST_TYPE_NAME + "\" "
          + "expected at function ARRAY_CONTAINS_ALL, but "
          + "\"" + arguments[ARRAY_IDX].getTypeName() + "\" "
          + "is found");
    }
    
    // Check if ARRAY_IDX argument is of category LIST
    if (!arguments[VALUE_IDX].getCategory().equals(Category.LIST)) {
      throw new UDFArgumentTypeException(VALUE_IDX,
          "\"" + org.apache.hadoop.hive.serde.serdeConstants.LIST_TYPE_NAME + "\" "
          + "expected at function ARRAY_CONTAINS_ALL, but "
          + "\"" + arguments[VALUE_IDX].getTypeName() + "\" "
          + "is found");
    }


    arrayOI = (ListObjectInspector) arguments[ARRAY_IDX];
    arrayElementOI = arrayOI.getListElementObjectInspector();

    valueOI = (ListObjectInspector) arguments[VALUE_IDX];
    valueElementOI = valueOI.getListElementObjectInspector();

    // Check if list element and value are of same type
    if (!ObjectInspectorUtils.compareTypes(arrayElementOI, valueElementOI)) {
      throw new UDFArgumentTypeException(VALUE_IDX,
          "\"" + arrayElementOI.getTypeName() + "\""
          + " expected at function ARRAY_CONTAINS, but "
          + "\"" + valueElementOI.getTypeName() + "\""
          + " is found");
    }

   
    result = new BooleanWritable(false);

    return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
  }



  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {

    result.set(false);

    Object array = arguments[ARRAY_IDX].get();
    Object arrayValue = arguments[VALUE_IDX].get();

    int arrayLength = arrayOI.getListLength(array);
    int arrayValLength = valueOI.getListLength(arrayValue);

    // Check if array is null or empty or value is null
    if (arrayValLength <=0 || arrayLength <= 0) {
      return result;
    }

    // Compare the value to each element of array until a match is found
    boolean match = false;
    for (int j=0; j<arrayValLength; ++j) {
    	match = false;
    	Object value = arrayOI.getListElement(arrayValue, j);
    	for (int i=0; i<arrayLength; ++i) {
    	      Object listElement = arrayOI.getListElement(array, i);
    	      if (listElement != null && value != null) {
    	        if (ObjectInspectorUtils.compare(value, valueElementOI,
    	            listElement, arrayElementOI) == 0) {
    	          match= true;
    	          break;
    	        }
    	      }
    	}
    	if (match == false) {
    		break;
    	}
    }
    result.set(match);
    return result;
  }

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length == ARG_COUNT);
    return "array_contains(" + children[ARRAY_IDX] + ", "
              + children[VALUE_IDX] + ")";
  }
}
