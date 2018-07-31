package pqe.ecms.aws;

import com.amazonaws.services.s3.model.S3Object;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.stream.Collectors;

public class AmazonS3Helper {

	public static String readLinesFromAmazon(S3Object s3Object) throws IOException {
		String input;
		try (BufferedReader buffer = new BufferedReader(new InputStreamReader(s3Object.getObjectContent()))) {
			input = buffer.lines().collect(Collectors.joining("\n"));
		}
		return input;
	}

}
