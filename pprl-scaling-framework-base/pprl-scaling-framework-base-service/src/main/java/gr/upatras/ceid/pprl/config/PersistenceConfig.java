package gr.upatras.ceid.pprl.config;

import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.orm.hibernate5.LocalSessionFactoryBean;

import javax.sql.DataSource;
import java.util.Properties;

@Configuration
public class PersistenceConfig {
    private static final Logger LOG = LoggerFactory.getLogger(PersistenceConfig.class);

    @Value("${database.host}")
    private String dbHost;
    @Value("${database.username}")
    private String dbUser;
    @Value("${database.password}")
    private String dbPassword;

//    @Bean
//    @Conditional(PPRLCLusterCondition.class)
//    public DataSource configureDatasource() {
//        LOG.info(String.format("Database as DataSource [url=%s,username=%s,password=%s]",
//                String.format("jdbc:mysql://%s:3306",dbHost),
//                dbUser,
//                String.format("%c...%c",
//                        dbPassword.charAt(0),dbPassword.charAt(dbPassword.length()-1))));
//
//        DriverManagerDataSource ds = new DriverManagerDataSource();
//        ds.setDriverClassName("com.mysql.jdbc.Driver");
//        ds.setUrl(String.format("jdbc:mysql://%s:3306",dbHost));
//        ds.setUsername(dbUser);
//        ds.setPassword(dbPassword);
//        return ds;
//    }


//    @Bean
//    @Conditional(PPRLCLusterCondition.class)
//    public Properties configureHibernateProperties() {
//        LOG.info("Setting up hibernate properties.");
//        Properties properties = new Properties();
//        properties.put("hibernate.dialect", "org.hibernate.dialect.MySQLDialect");
//        properties.put("hibernate.show_sql", true);
//        properties.put("hibernate.hbm2ddl.auto", "update");
//        return properties;
//    }

//    @Bean
//    @Conditional(PPRLCLusterCondition.class)
//    public SessionFactory configureSessionFactory()
//    {
//        LOG.info("Setting up SessionFactory.");
//        LocalSessionFactoryBean lsfb = new LocalSessionFactoryBean();
//        lsfb.setDataSource(configureDatasource());
//        lsfb.setHibernateProperties(configureHibernateProperties());
//        //lsfb.setMappingResources("Bank.hbm.xml");
//        return lsfb.getObject();
//    }
}
