package pqe.ecms.title.exceptions;

public class TitleContentException extends Exception {
	private static final long serialVersionUID = 1L;

	public TitleContentException() {
		super();
	}
	
	public TitleContentException(final String message) {
		super(message);
	}
	
	public TitleContentException(final String message, final Throwable cause){
		super(message, cause);
	}

	public TitleContentException(final Throwable cause) {
		super(cause);
	}
}
