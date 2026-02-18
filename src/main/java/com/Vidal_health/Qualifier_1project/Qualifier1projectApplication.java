package com.Vidal_health.Qualifier_1project;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;
import java.util.HashMap;
import java.util.Map;

@SpringBootApplication
public class Qualifier1projectApplication implements CommandLineRunner {

	public static void main(String[] args) {
		SpringApplication.run(Qualifier1projectApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {

		RestTemplate restTemplate = new RestTemplate();
		String generateUrl =
				"https://bfhldevapigw.healthrx.co.in/hiring/generateWebhook/JAVA";

		Map<String, String> requestBody = new HashMap<>();
		requestBody.put("name", "Manoj Gowda");
		requestBody.put("regNo", "1SG21EC062");
		requestBody.put("email", "manojgowdaam280504@gmail.com");

		ResponseEntity<Map> response =
				restTemplate.postForEntity(generateUrl, requestBody, Map.class);

		Map<String, String> responseBody = response.getBody();

		String webhookUrl = responseBody.get("webhook");
		String accessToken = responseBody.get("accessToken");

		System.out.println("Webhook URL: " + webhookUrl);
		System.out.println("Access Token: " + accessToken);
//my regno ends with 62 so its even i have solved the second question
		String finalSqlQuery = """
				SELECT 
				    DEPARTMENT_NAME,
				    AVERAGE_AGE,
				    LISTAGG(EMPLOYEE_NAME, ', ') 
				        WITHIN GROUP (ORDER BY EMP_ID) AS EMPLOYEE_LIST
				FROM (
				    SELECT 
				        d.DEPARTMENT_ID,
				        d.DEPARTMENT_NAME,
				        e.EMP_ID,
				        e.FIRST_NAME || ' ' || e.LAST_NAME AS EMPLOYEE_NAME,
				        TRUNC(MONTHS_BETWEEN(SYSDATE, e.DOB) / 12) AS AGE,
				        AVG(TRUNC(MONTHS_BETWEEN(SYSDATE, e.DOB) / 12)) 
				            OVER (PARTITION BY d.DEPARTMENT_ID) AS AVERAGE_AGE,
				        ROW_NUMBER() OVER (
				            PARTITION BY d.DEPARTMENT_ID 
				            ORDER BY e.EMP_ID
				        ) AS rn
				    FROM EMPLOYEE e
				    JOIN DEPARTMENT d ON e.DEPARTMENT = d.DEPARTMENT_ID
				    JOIN PAYMENTS p ON e.EMP_ID = p.EMP_ID
				    WHERE p.AMOUNT > 70000
				)
				WHERE rn <= 10
				GROUP BY DEPARTMENT_ID, DEPARTMENT_NAME, AVERAGE_AGE
				ORDER BY DEPARTMENT_ID DESC
				""";
		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);
		headers.set("Authorization", accessToken);
		Map<String, String> finalBody = new HashMap<>();
		finalBody.put("finalQuery", finalSqlQuery);
		HttpEntity<Map<String, String>> entity =new HttpEntity<>(finalBody, headers);
		ResponseEntity<String> finalResponse =
				restTemplate.postForEntity(webhookUrl, entity, String.class);
		System.out.println("Submission Response: " + finalResponse.getBody());
		System.out.println("API test completed successfully");
	}
}
