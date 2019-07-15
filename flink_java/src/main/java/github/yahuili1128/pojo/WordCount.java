package github.yahuili1128.pojo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Description : socket wordcount pojo
 * @Author : LiYahui
 * @Date : 2019-06-25 16:49
 * @Version : V1.0
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WordCount {
	public String word;
	public long count;

}
