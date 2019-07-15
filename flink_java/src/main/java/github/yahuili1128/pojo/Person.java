package github.yahuili1128.pojo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Description : 测试数据
 * @Author : LiYahui
 * @Date : 2019-07-15 11:55
 * @Version : V1.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Person {
	public String name;
	public String address;
	public String department;
	public String level;
}
