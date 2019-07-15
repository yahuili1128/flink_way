package github.yahuili1128.mockup;

import static github.yahuili1128.util.KafkaProducerUtil.sendMockUpModel;

/**
 * @Description : 向kafka中发送topic
 * @Author : LiYahui
 * @Date : 2019-07-15 17:25
 * @Version : V1.0
 */
public class MockUpTopic {
	public static void main(String[] args) throws InterruptedException {
		sendMockUpModel();
	}

}
