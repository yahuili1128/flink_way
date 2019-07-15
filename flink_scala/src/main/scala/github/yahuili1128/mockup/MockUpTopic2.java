package github.yahuili1128.mockup;

import lombok.val;

import static github.yahuili1128.util.KafkaProducerUtil.sendMockUpModelTopic2;

/**
 * @Description : 向kafka中发送topic2
 * @Author : LiYahui
 * @Date : 2019-07-15 17:27
 * @Version : V1.0
 */
public class MockUpTopic2 {
	public static void main(String[] args) throws InterruptedException {
		sendMockUpModelTopic2();


	}
}
