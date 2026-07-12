package za.co.fnb.dcre.ixr;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;
import za.co.fnb.dcre.platform.batch.ExitCodeMain;
import za.co.fnb.dcre.platform.persistence.JdbcConfig;

@SpringBootApplication
@Import(JdbcConfig.class)
public class IxrApplication {

    public static void main(String[] args) {
        ExitCodeMain.run(IxrApplication.class, args);
    }
}
