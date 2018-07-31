package pqe.ecms.reindexer.workflow;

import pqe.ecms.client.editorialstorage.domain.MappedDocument;
import pqe.ecms.rest.message.document.Document;

public class IndexingDocument {

	private Long documentId;
	private Long documentVersionId;
	private Document document;

	private MappedDocument mappedDocument;

	private String bucket;
	private String s3Partition;

	public Long getDocumentId() {
		return documentId;
	}

	public void setDocumentId(Long documentId) {
		this.documentId = documentId;
	}

	public Document getDocument() {
		return document;
	}

	public void setDocument(Document document) {
		this.document = document;
	}

	public MappedDocument getMappedDocument() {
		return mappedDocument;
	}

	public void setMappedDocument(MappedDocument mappedDocument) {
		this.mappedDocument = mappedDocument;
	}

	public String getS3Partition() {
		return s3Partition;
	}

	public void setS3Partition(String s3Partition) {
		this.s3Partition = s3Partition;
	}

	public Long getDocumentVersionId() {
		return documentVersionId;
	}

	public void setDocumentVersionId(Long documentVersionId) {
		this.documentVersionId = documentVersionId;
	}

	public String getBucket() {
		return bucket;
	}

	public void setBucket(String bucket) {
		this.bucket = bucket;
	}

	@Override
	public String toString() {
		return "IndexingDocument{" +
				"documentId=" + documentId +
				", documentVersionId=" + documentVersionId +
				", bucket='" + bucket + '\'' +
				", s3Partition='" + s3Partition + '\'' +
				'}';
	}
}
