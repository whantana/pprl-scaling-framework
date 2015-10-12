package gr.upatras.ceid.pprl.datasets.util;

import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.mapreduce.Job;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.ClassUtils;

public class AvroJobInitializingBean implements InitializingBean {

    private Job job;
    private String avroInputKey = null;
    private String avroInputValue = null;
    private String avroMapOutputKey = null;
    private String avroMapOutputValue = null;
    private String avroOutputKey = null;
    private String avroOutputValue = null;
    private ClassLoader cl;

    /**
     * @param aJob a reference to the (avro) job to configure
     */
    public AvroJobInitializingBean(Job aJob) {
        this.job = aJob;
    }

    public void afterPropertiesSet() throws Exception {

        if (avroInputKey != null) {
            AvroJob.setInputKeySchema(job, resolveClass(avroInputKey).newInstance().getSchema());
        }

        if (avroInputValue != null) {
            AvroJob.setInputValueSchema(job, resolveClass(avroInputValue).newInstance().getSchema());
        }

        if (avroMapOutputKey != null) {
            AvroJob.setMapOutputKeySchema(job, resolveClass(avroMapOutputKey).newInstance().getSchema());
        }

        if (avroMapOutputValue != null) {
            Class<? extends IndexedRecord> c = resolveClass(avroMapOutputValue);
            IndexedRecord o = c.newInstance();
            AvroJob.setMapOutputValueSchema(job, o.getSchema());
        }

        if (avroOutputKey != null) {
            AvroJob.setOutputKeySchema(job, resolveClass(avroOutputKey).newInstance().getSchema());
        }

        if (avroOutputValue != null) {
            AvroJob.setOutputValueSchema(job, resolveClass(avroOutputValue).newInstance().getSchema());
        }
    }

    public void setAvroInputKey(String avroInputKey) {
        this.avroInputKey = avroInputKey;
    }

    public void setAvroInputValue(String avroInputValue) {
        this.avroInputValue = avroInputValue;
    }

    public void setAvroMapOutputKey(String avroMapOutputKey) {
        this.avroMapOutputKey = avroMapOutputKey;
    }

    public void setAvroMapOutputValue(String avroMapOutputValue) {
        this.avroMapOutputValue = avroMapOutputValue;
    }

    public void setAvroOutputKey(String avroOutputKey) {
        this.avroOutputKey = avroOutputKey;
    }

    public void setAvroOutputValue(String avroOutputValue) {
        this.avroOutputValue = avroOutputValue;
    }

    @SuppressWarnings("unchecked")
    protected Class<? extends IndexedRecord> resolveClass(String className) {
        return (Class<? extends IndexedRecord>) ClassUtils.resolveClassName(className, getBeanClassLoader());
    }

    protected ClassLoader getBeanClassLoader() {
        if (cl == null) {
            cl = org.springframework.util.ClassUtils.getDefaultClassLoader();
        }
        return cl;
    }

    protected void setBeanClassLoader(ClassLoader beanClassLoader) {
        this.cl = beanClassLoader;
    }
}
