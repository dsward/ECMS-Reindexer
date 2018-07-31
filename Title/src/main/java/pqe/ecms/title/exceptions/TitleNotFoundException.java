package pqe.ecms.title.exceptions;

public class TitleNotFoundException extends TitleContentException {
	private static final long serialVersionUID = 1L;

	public TitleNotFoundException() {
		super();
	}

	public TitleNotFoundException(final String message) {
		super(message);
	}

	public TitleNotFoundException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public TitleNotFoundException(final Throwable cause) {
		super(cause);
	}
}
