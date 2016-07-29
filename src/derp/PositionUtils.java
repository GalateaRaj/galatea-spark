package derp;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class PositionUtils {

	public static class Position implements Serializable {
		private static final long serialVersionUID = 7904127661383626801L;

		String acct;
		String inst;
		Long qty;

		public Position(final String acct_, final String inst_, final Long qty_) {
			acct = acct_;
			inst = inst_;
			qty = qty_;
		}
	}

	
	public static List<Position> createTestPositions() {
		int custCount = 100;
		int instCount = 10000;
		int posCount = 1000000;

		// Create my input data
		List<Position> posList = new ArrayList<Position>(posCount);
		for (int i = 0; i < posCount; i++) {
			String cust = "Cust-" + ThreadLocalRandom.current().nextInt(0, custCount);
			String inst = "Inst-" + ThreadLocalRandom.current().nextInt(0, instCount);
			long qty = ThreadLocalRandom.current().nextInt(0, 500000);

			posList.add(new Position(cust, inst, qty));
		}
		
		return posList;

	}
}
