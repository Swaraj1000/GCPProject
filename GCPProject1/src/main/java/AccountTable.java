
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

@DefaultSchema(JavaFieldSchema.class)
public class AccountTable {
    private int id;
    private String name;
    private String surname;

    public AccountTable(int id,String name,String surname){
        this.id = id;
        this.name = name;
        this.surname = surname;
    }
}