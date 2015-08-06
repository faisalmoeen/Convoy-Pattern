package streaming;

import java.util.ArrayList;
import java.util.List;

import base.Convoy;

public class Eval {
	List<Convoy> convoyListOriginal;
	List<Convoy> convoyListTest; 
	public ConvoyComparisonMetric evaluate(String origFilePath, String testFilePath){
		convoyListOriginal = getConvoys(origFilePath);
		convoyListTest = getConvoys(testFilePath);
		ConvoyComparisonMetric evm = evaluateResults(convoyListOriginal, convoyListTest);
		return evm;
	}

	public ConvoyComparisonMetric evaluateResults(List<Convoy> convoyListOriginal, List<Convoy> convoyListTest) {
		List<ConvoyComparisonMetric> metrics = new ArrayList<>();
		for(Convoy vOrig:convoyListOriginal){
			Convoy vClosest = findClosestConvoy(vOrig,convoyListTest);
			ConvoyComparisonMetric metric = convoyComparisonMetric(vOrig, vClosest);
			metrics.add(metric);
		}
		return average(metrics);
	}

	private ConvoyComparisonMetric average(List<ConvoyComparisonMetric> metrics) {
		EvaluationMetric objEval = new EvaluationMetric(0, 0);
		EvaluationMetric temporalEval = new EvaluationMetric(0, 0);;
		EvaluationMetric avgEval = new EvaluationMetric(0, 0);;
		for(ConvoyComparisonMetric ccm:metrics){
			avgEval.accuracy+=ccm.avgEval.accuracy;
			avgEval.recall+=ccm.avgEval.recall;
			objEval.accuracy+=ccm.objEval.accuracy;
			objEval.recall+=ccm.objEval.recall;
			temporalEval.accuracy+=ccm.temporalEval.accuracy;
			temporalEval.recall+=ccm.temporalEval.recall;
		}
		return new ConvoyComparisonMetric(avgEval.accuracy/metrics.size(), avgEval.recall/metrics.size(), 
				objEval.accuracy/metrics.size(), objEval.recall/metrics.size(),
				temporalEval.accuracy/metrics.size(), temporalEval.recall/metrics.size());
	}

	private double convoyComparisonScore(Convoy trueConvoy, Convoy testConvoy) {
		double objTimeRatio = ((double)(trueConvoy.getObjs().size()))/trueConvoy.lifetime();
		EvaluationMetric objEvalMetric = findObjectsAccuracyRecall(trueConvoy, testConvoy);
		double objFMeasure = objEvalMetric.getFMeasure();
		EvaluationMetric temporalEvalMetric = findTemporalAccuracyRecall(trueConvoy, testConvoy);
		double temporalFMeasure = temporalEvalMetric.getFMeasure();
		return objFMeasure*(1-objTimeRatio) + temporalFMeasure*(objTimeRatio);
	}
	
	private ConvoyComparisonMetric convoyComparisonMetric(Convoy trueConvoy, Convoy testConvoy) {
		double objTimeRatio = ((double)(trueConvoy.getObjs().size()))/trueConvoy.lifetime();
		EvaluationMetric objEvalMetric = findObjectsAccuracyRecall(trueConvoy, testConvoy);
		EvaluationMetric temporalEvalMetric = findTemporalAccuracyRecall(trueConvoy, testConvoy);
		double avgAccuracy = objEvalMetric.accuracy*(1-objTimeRatio) + temporalEvalMetric.accuracy*(objTimeRatio);
		double avgRecall = objEvalMetric.recall*(1-objTimeRatio) + temporalEvalMetric.recall*(objTimeRatio);
		return new ConvoyComparisonMetric(avgAccuracy, avgRecall, 
				objEvalMetric.accuracy, objEvalMetric.recall, 
				temporalEvalMetric.accuracy,temporalEvalMetric.recall);
	}

	private List<Convoy> getConvoys(String filePath1) {
		// TODO Auto-generated method stub
		return null;
	}
	
	private Convoy findClosestConvoy(Convoy v, List<Convoy> convoyList){
		Convoy closestConvoy = null;
		double minScore = 0;
		for(Convoy v2:convoyList){
			double score = convoyComparisonScore(v, v2);
			if(score>minScore){
				minScore = score;
				closestConvoy = v2;
			}
		}
		return closestConvoy;
	}
	
	private EvaluationMetric findTemporalAccuracyRecall(Convoy trueConvoy, Convoy testConvoy){
		long temporalIntersection = trueConvoy.temporalIntersectionSize(testConvoy);
		double accuracy=0;
		double recall = 0;
		if(temporalIntersection!=0){
			accuracy = (double)temporalIntersection/testConvoy.lifetime();
			recall = (double)temporalIntersection/trueConvoy.lifetime();
		}
		return new EvaluationMetric(accuracy,recall);
	}
	
	private EvaluationMetric findObjectsAccuracyRecall(Convoy trueConvoy, Convoy testConvoy){
		long objIntersection = trueConvoy.objIntersectionSize(testConvoy);
		double accuracy=0;
		double recall = 0;
		if(objIntersection!=0){
			accuracy = (double)objIntersection/testConvoy.size();
			recall = (double)objIntersection/trueConvoy.size();
		}
		return new EvaluationMetric(accuracy,recall);
	}
	
}
