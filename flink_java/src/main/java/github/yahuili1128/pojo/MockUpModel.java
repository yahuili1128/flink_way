package github.yahuili1128.pojo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Description :向kafka中发送测试数据
 * @Author : LiYahui
 * @Date : 2019-06-25 22:19
 * @Version : V1.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class MockUpModel {
	public String name;
	public String gender;
	public long timestamp;
	public int age;

}
