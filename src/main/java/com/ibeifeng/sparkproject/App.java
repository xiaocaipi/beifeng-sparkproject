package com.ibeifeng.sparkproject;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {


        String jsonStr = "{\"alipay_trade_app_pay_response\":{\"code\":\"10000\",\"msg\":\"Success\",\"app_id\":\"2017080908108654\",\"auth_app_id\":\"2017080908108654\",\"charset\":\"utf-8\",\"timestamp\":\"2017-08-17 19:56:25\",\"total_amount\":\"0.01\",\"trade_no\":\"2017081721001004960228389410\",\"seller_id\":\"2088721744204654\",\"out_trade_no\":\"20170817195615000792\"},\"sign\":\"qjDQPXWaipDNmzABuLpdMvJxDclsKssiFc6HO8/3+ASfZQDe8iQcPrPKp8cizPlMioFrzb+eMoptNBCPsk2CEk0580gVb0QzA/Bd+3/zK5lp2H8p/JLEvUe5vJql/B1ADIUowhWBZzETQHyheGpyTJqgvPn9wxbcHD+hLtESQv+9RROzixLiaLB0vf8kRDWAxbpJC9GB70KCKvYbdbb9/UbyQCLU+6HSKE/C5TO4MziIFvgm3hZNfNptqBHQ6VPBabsCIOkJf9L2yPa48mwnvwh6tGTr2p6dnvgw+lRqHXluJpAfwW+AIubfarPTHH4mFFTbL2S2iGfnVbd8RsKp9g==\",\"sign_type\":\"RSA2\"}";
        JSONObject obj = JSONObject.parseObject(jsonStr, Feature.OrderedField);
//        System.out.println(obj.getJSONObject("alipay_trade_app_pay_response").toJSONString());
        System.out.println(obj.getString("alipay_trade_app_pay_response"));
    }
}
