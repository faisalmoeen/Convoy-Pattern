package streaming;

public class EvaluationMetric {

	public double accuracy;
	public double recall;

	public EvaluationMetric(double accuracy, double recall) {
		this.accuracy = accuracy;
		this.recall = recall;
	}
	
	public double getFMeasure() {
		if(accuracy!=0 && recall!=0){
			return 2*accuracy*recall/(accuracy+recall);
		}
		else
			return 0;
	}

}
