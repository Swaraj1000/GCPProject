
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

@DefaultSchema(JavaFieldSchema.class)
public class AccountTable {
    private int id;
    private String Name;
    private String Surname;

    public AccountTable(int id,String Name,String Surname){
        this.id = id;
        this.Name = Name;
        this.Surname = Surname;
    }
}