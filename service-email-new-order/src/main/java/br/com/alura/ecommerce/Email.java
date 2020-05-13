package br.com.alura.ecommerce;

public class Email {

	private String subject;
	private String body;
	
	public Email(String subject, String body) {
		this.subject = subject;
		this.body = body;
	}

	@Override
	public String toString() {
		return "Email [subject=" + subject + ", body=" + body + "]";
	}
}
