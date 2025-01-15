use tantivy::query::{EnableScoring, Explanation, Query, QueryClone, Scorer, Weight};
use tantivy::{DocId, DocSet, Score, SegmentReader};

#[derive(Debug)]
/// A query that negates/inverts the match result of the inner query.
pub struct NegateQuery(pub Box<dyn Query>);

impl Clone for NegateQuery {
    fn clone(&self) -> Self {
        Self(self.0.box_clone())
    }
}

impl Query for NegateQuery {
    fn weight(
        &self,
        enable_scoring: EnableScoring<'_>,
    ) -> tantivy::Result<Box<dyn Weight>> {
        let inner = self.0.weight(enable_scoring)?;
        Ok(Box::new(NegateWeight(inner)))
    }
}

struct NegateWeight(Box<dyn Weight>);

impl Weight for NegateWeight {
    fn scorer(
        &self,
        reader: &SegmentReader,
        boost: Score,
    ) -> tantivy::Result<Box<dyn Scorer>> {
        let inner = self.0.scorer(reader, boost)?;
        Ok(Box::new(NegateScorer(inner)))
    }

    fn explain(
        &self,
        reader: &SegmentReader,
        doc: DocId,
    ) -> tantivy::Result<Explanation> {
        let mut scorer = self.0.scorer(reader, 1.0)?;
        if scorer.seek(doc) != doc {
            return Ok(Explanation::new(
                "NegateQuery inverts result of inner query (original=0.0)",
                1.0,
            ));
        }

        let score = scorer.score();
        let explanation = if score > 0.0 {
            Explanation::new_with_string(
                format!(
                    "NegateQuery inverts result of inner query (original_score={score})"
                ),
                0.0,
            )
        } else {
            Explanation::new(
                "NegateQuery inverts result of inner query (original_score=0.0)",
                1.0,
            )
        };

        Ok(explanation)
    }
}

struct NegateScorer(Box<dyn Scorer>);

impl DocSet for NegateScorer {
    fn advance(&mut self) -> DocId {
        self.0.advance()
    }

    fn doc(&self) -> DocId {
        self.0.doc()
    }

    fn size_hint(&self) -> u32 {
        self.0.size_hint()
    }
}

impl Scorer for NegateScorer {
    fn score(&mut self) -> Score {
        let score = self.0.score();
        if score > 0.0 {
            0.0
        } else {
            1.0
        }
    }
}
