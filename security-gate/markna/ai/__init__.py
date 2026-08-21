"""AI reasoning layer.

This package is deliberately isolated from the deterministic scanners. Nothing in
here can produce blocking evidence: the policy engine marks every finding whose
``ai_generated`` flag is set as advisory unless an operator explicitly opts in.
"""

from .reviewer import AiReviewError, AiReviewer, AiReviewResult

__all__ = ["AiReviewer", "AiReviewResult", "AiReviewError"]
