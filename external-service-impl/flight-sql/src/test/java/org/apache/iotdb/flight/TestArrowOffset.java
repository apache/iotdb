import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;

public class TestArrowOffset {
    public static void main(String[] args) {
        try (BufferAllocator allocator = new RootAllocator();
                VarCharVector vector = new VarCharVector("test", FieldType.nullable(new ArrowType.Utf8()), allocator)) {
            vector.allocateNew();

            // Set index 0 to a string
            vector.setSafe(0, "hello".getBytes());
            // Set index 1 to null
            vector.setNull(1);
            // Set index 2 to another string
            vector.setSafe(2, "world".getBytes());

            vector.setValueCount(3);

            System.out.println("Offset at 0: " + vector.getOffsetBuffer().getInt(0));
            System.out.println("Offset at 1: " + vector.getOffsetBuffer().getInt(4));
            System.out.println("Offset at 2: " + vector.getOffsetBuffer().getInt(8));
            System.out.println("Offset at 3: " + vector.getOffsetBuffer().getInt(12));

            VectorUnloader unloader = new VectorUnloader(new org.apache.arrow.vector.VectorSchemaRoot(
                    java.util.Collections.singletonList(vector.getField()),
                    java.util.Collections.singletonList(vector),
                    3));
            try (ArrowRecordBatch batch = unloader.getRecordBatch()) {
                System.out.println("Record batch length: " + batch.computeBodyLength());
            }
        }
    }
}
