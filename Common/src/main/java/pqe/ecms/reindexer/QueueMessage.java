package pqe.ecms.reindexer;

import java.util.ArrayList;
import java.util.List;

public class QueueMessage {

	private List<Long> documentIdList = new ArrayList<>();

	public QueueMessage() {}

	public QueueMessage(List<Long> list) {
		documentIdList = new ArrayList<>(list.size());
		documentIdList.addAll(list);
	}

	public List<Long> getDocumentIdList() {
		return documentIdList;
	}

	public void setDocumentIdList(List<Long> documentIdList) {
		this.documentIdList = documentIdList;
	}
}
