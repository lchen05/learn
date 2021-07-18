package trainmr.com.cn.my;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

//1 实现writable接口

public class FlowBean implements Writable {
	//上传流量
	private Long upFlow;
	//上传流量
    private Long downFlow;
    //流量总和
    private Long sumFlow;
    
    //必须要有，反序列化要调用空参构造器
    public FlowBean() {
    }

    public Long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(Long upFlow) {
        this.upFlow = upFlow;
    }

    public Long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(Long downFlow) {
        this.downFlow = downFlow;
    }

    public Long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(Long sumFlow) {
        this.sumFlow = sumFlow;
    }

    public void set(Long upFlow, Long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }

    @Override
    public String toString() {
        return "FlowBean=[\t" +
                "upFlow="+upFlow+"\t"+"downFlow="+downFlow+"\t"+"sumFlow="+sumFlow+"]";
    }

    /**
     * 序列化方法
     * @param dataOutput
     * @throws IOException
     */
    //@Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);
    }

    /**
     * 反序列化方法
     * @param dataInput
     * @throws IOException
     */
    //@Override
    public void readFields(DataInput dataInput) throws IOException {
        upFlow = dataInput.readLong();
        downFlow = dataInput.readLong();
        sumFlow = dataInput.readLong();
    }
}
