import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class ageGrouper extends EvalFunc<Integer> {

	@Override
	public Integer exec(Tuple input) throws IOException {
		
		if (input == null || input.size() == 0) return null;
		else {
			
			try {
				
				Integer age = (Integer) input.get(0);
				
				if (age < 18) return (Integer) 18;
				if (age >= 18 && age <= 24) return (Integer) 24;
				if (age >= 25 && age <= 34) return (Integer) 34;
				if (age >= 35 && age <= 44) return (Integer) 44;
				if (age >= 45 && age <= 49) return (Integer) 49;
				if (age >= 50 && age <= 56) return (Integer) 56;
				if (age > 56) return (Integer) 57;
				
				else return null;
				
			} catch (Exception e) {
				
				throw new IOException("Something went during determining an age group:", e);
				
			}
			
		}
		
	}

	
}