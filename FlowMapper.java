package trainmr.com.cn.my;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    private Text phone = new Text();
    private FlowBean flowBean = new FlowBean();
    //@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] datas = value.toString().split("\t");
        phone.set(datas[1]);
        long upFlow = Long.parseLong(datas[datas.length -2]);
        long downFlow = Long.parseLong(datas[datas.length -1]);
        flowBean.set(upFlow, downFlow);
        context.write(phone, flowBean);
    }
}
