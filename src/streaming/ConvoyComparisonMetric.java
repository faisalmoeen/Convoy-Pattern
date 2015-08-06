package streaming;

public class ConvoyComparisonMetric {
	public EvaluationMetric objEval = new EvaluationMetric(0, 0);
	public EvaluationMetric temporalEval = new EvaluationMetric(0, 0);;
	public EvaluationMetric avgEval = new EvaluationMetric(0, 0);;
	public ConvoyComparisonMetric(double avgAccuracy, double avgRecall, double objAccuracy, double objRecall, double temporalAccuracy, double temporalRaceall) {
		this.avgEval.accuracy=avgAccuracy;
		this.avgEval.recall=avgRecall;
		this.objEval.accuracy = objAccuracy;
		this.objEval.recall = objRecall;
		this.temporalEval.accuracy = temporalAccuracy;
		this.temporalEval.recall = temporalRaceall;
	}
}
