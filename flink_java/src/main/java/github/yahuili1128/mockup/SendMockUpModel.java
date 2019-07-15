package github.yahuili1128.mockup;


import static github.yahuili1128.util.KafkaProducerUtil.sendMockUpModel;
import static github.yahuili1128.util.KafkaProducerUtil.sendPerson;


/**
 * @Description :  将消息以json的形式发送到kafka中
 * @Author : LiYahui
 * @Date : 2019-06-25 22:43
 * @Version : V1.0
 */
public class SendMockUpModel {
	public static void main(String[] args) throws InterruptedException {
		sendMockUpModel();
		//    {"age":29,"gender":"female","name":"yahui","timestamp":1563068755206}
		//		sendPerson();
	}
}
