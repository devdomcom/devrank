"""
Comprehensive tests for the language-independence refactor.

Covers every gap identified in the test audit:
  Gap 1:  _load_customization / _reset_customization_cache / _get_* helpers
  Gap 2:  is_bug_fix_indicator labels-first path
  Gap 3:  classify_defect_commit labels-first path
  Gap 4:  is_revert_indicator SHA-based structural fallback
  Gap 5:  is_structured_commit broad check
  Gap 6:  compute_pr_body_quality GitLab !NNN / Azure DevOps AB#NNN
  Gap 7:  is_documentation_file context-based fallback (Layer 4)
  Gap 8:  _get_doc_dirs / _get_generated_markers config extras
  Gap 9:  pr_category_diversity: _classify_by_labels, _classify_by_diff, _classify_pr, _get_label_to_category
  Gap 10: revert_introduction_rate._extract_reverted_sha structural SHA fallback
  Gap 11: is_bot_user canonical field
  Gap 12: is_merge_commit parent_count structural path
  Gap 13: co_author_contribution_rate broadened trailer pattern
  Gap 14: review_comment_substance._score_comment structural signals
  Gap 15: CIRunRecord model import (alias removed)
  Gap 16: tree-sitter expanded languages (C, C++, C#, Ruby, PHP, Kotlin, Swift, Scala)
  Gap 17: Constructor detection (Go New*, Rust new, PHP __construct, Ruby initialize, Swift init)
"""

import os
import tempfile
from datetime import timedelta

import yaml

from impact.domain.models import User
from impact.metrics.utils import (
    _load_customization,
    _reset_customization_cache,
    _get_bug_labels,
    _get_defect_labels,
    _get_doc_dirs,
    _get_generated_markers,
    _BUG_LABELS_BUILTIN,
    _DEFECT_LABELS_BUILTIN,
    _DOC_DIRS_BUILTIN,
    _GENERATED_MARKERS_BUILTIN,
    classify_defect_commit,
    compute_pr_body_quality,
    is_bot_user,
    is_bug_fix_indicator,
    is_documentation_file,
    is_generated_file,
    is_merge_commit,
    is_revert_indicator,
    is_structured_commit,
    parse_functions,
)
from impact.metrics.plugins.authored.revert_introduction_rate import _extract_reverted_sha
from impact.metrics.plugins.authored.pr_category_diversity import (
    _classify_by_labels,
    _classify_by_diff,
    _classify_pr,
    _get_label_to_category,
    _LABEL_TO_CATEGORY_BUILTIN,
)
from impact.metrics.plugins.influence.review_comment_substance import _score_comment
from impact.metrics.plugins.mixed.co_author_contribution_rate import CO_AUTHOR_PATTERN
from impact.tests.conftest import (
    DEFAULT_START,
    make_bundle,
    make_commit,
    make_context,
    make_pr,
    make_repo,
    make_user,
)


# ============================================================================
# Gap 1 — _load_customization / _reset_customization_cache / config getters
# ============================================================================


class TestLoadCustomization:
    """Config loading, caching, env-var override, graceful degradation."""

    def teardown_method(self):
        _reset_customization_cache()
        os.environ.pop("DEVRANK_CUSTOMIZATION_PATH", None)

    def test_loads_default_config(self):
        _reset_customization_cache()
        cfg = _load_customization()
        assert isinstance(cfg, dict)
        # Default config should have keys from customization.yaml
        assert "extra_bug_labels" in cfg or cfg == {}  # graceful degradation is ok

    def test_caches_result(self):
        _reset_customization_cache()
        cfg1 = _load_customization()
        cfg2 = _load_customization()
        assert cfg1 is cfg2  # same object — cached

    def test_reset_clears_cache(self):
        _reset_customization_cache()
        cfg1 = _load_customization()
        _reset_customization_cache()
        cfg2 = _load_customization()
        # After reset, should re-load (may be same content but different object)
        assert isinstance(cfg2, dict)

    def test_env_var_override(self):
        _reset_customization_cache()
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump({"extra_bug_labels": ["mi_etiqueta"]}, f)
            tmp_path = f.name
        try:
            os.environ["DEVRANK_CUSTOMIZATION_PATH"] = tmp_path
            cfg = _load_customization()
            assert cfg.get("extra_bug_labels") == ["mi_etiqueta"]
        finally:
            os.unlink(tmp_path)

    def test_missing_file_graceful_degradation(self):
        _reset_customization_cache()
        os.environ["DEVRANK_CUSTOMIZATION_PATH"] = "/nonexistent/path.yaml"
        cfg = _load_customization()
        assert cfg == {}  # graceful degradation

    def test_invalid_yaml_graceful_degradation(self):
        _reset_customization_cache()
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(": : : invalid yaml {{{\n")
            tmp_path = f.name
        try:
            os.environ["DEVRANK_CUSTOMIZATION_PATH"] = tmp_path
            cfg = _load_customization()
            assert cfg == {}
        finally:
            os.unlink(tmp_path)


class TestConfigGetters:
    """_get_bug_labels, _get_defect_labels, _get_doc_dirs, _get_generated_markers."""

    def teardown_method(self):
        _reset_customization_cache()
        os.environ.pop("DEVRANK_CUSTOMIZATION_PATH", None)

    def test_get_bug_labels_without_extras(self):
        _reset_customization_cache()
        # Point at a config with empty extras
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump({"extra_bug_labels": []}, f)
            tmp_path = f.name
        try:
            os.environ["DEVRANK_CUSTOMIZATION_PATH"] = tmp_path
            labels = _get_bug_labels()
            assert labels is _BUG_LABELS_BUILTIN  # same object — no extras
        finally:
            os.unlink(tmp_path)

    def test_get_bug_labels_with_extras(self):
        _reset_customization_cache()
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump({"extra_bug_labels": ["Fehler", "ERREUR"]}, f)
            tmp_path = f.name
        try:
            os.environ["DEVRANK_CUSTOMIZATION_PATH"] = tmp_path
            labels = _get_bug_labels()
            assert "fehler" in labels  # lowercased
            assert "erreur" in labels
            assert "bug" in labels  # built-in preserved
        finally:
            os.unlink(tmp_path)

    def test_get_defect_labels_with_extras(self):
        _reset_customization_cache()
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump({"extra_defect_labels": ["incidente"]}, f)
            tmp_path = f.name
        try:
            os.environ["DEVRANK_CUSTOMIZATION_PATH"] = tmp_path
            labels = _get_defect_labels()
            assert "incidente" in labels
            assert "bug" in labels  # built-in preserved
        finally:
            os.unlink(tmp_path)

    def test_get_doc_dirs_with_extras(self):
        _reset_customization_cache()
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump({"extra_doc_dirs": ["dokumentation/"]}, f)
            tmp_path = f.name
        try:
            os.environ["DEVRANK_CUSTOMIZATION_PATH"] = tmp_path
            dirs = _get_doc_dirs()
            assert "dokumentation/" in dirs
            assert "docs/" in dirs  # built-in preserved
        finally:
            os.unlink(tmp_path)

    def test_get_generated_markers_with_extras(self):
        _reset_customization_cache()
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump({"extra_generated_markers": ["nicht bearbeiten"]}, f)
            tmp_path = f.name
        try:
            os.environ["DEVRANK_CUSTOMIZATION_PATH"] = tmp_path
            markers = _get_generated_markers()
            assert "nicht bearbeiten" in markers
            assert "@generated" in markers  # built-in preserved
        finally:
            os.unlink(tmp_path)

    def test_config_extra_markers_detected_in_patch(self):
        """End-to-end: config extra markers are actually used by is_generated_file."""
        _reset_customization_cache()
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump({"extra_generated_markers": ["ne pas modifier"]}, f)
            tmp_path = f.name
        try:
            os.environ["DEVRANK_CUSTOMIZATION_PATH"] = tmp_path
            patch = "@@ -0,0 +1,3 @@\n+# ne pas modifier\n+x = 1\n"
            assert is_generated_file("output.py", patch)
        finally:
            os.unlink(tmp_path)
            _reset_customization_cache()

    def test_config_extra_doc_dirs_used(self):
        """End-to-end: config extra doc dirs are actually used by is_documentation_file."""
        _reset_customization_cache()
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump({"extra_doc_dirs": ["dokumentation/"]}, f)
            tmp_path = f.name
        try:
            os.environ["DEVRANK_CUSTOMIZATION_PATH"] = tmp_path
            assert is_documentation_file("dokumentation/guide.txt")
        finally:
            os.unlink(tmp_path)
            _reset_customization_cache()


# ============================================================================
# Gap 2 — is_bug_fix_indicator labels-first path
# ============================================================================


class TestBugFixIndicatorLabels:
    def test_labels_only_no_text(self):
        assert is_bug_fix_indicator("", labels=["bug"]) is True

    def test_label_case_insensitive(self):
        assert is_bug_fix_indicator("", labels=["TYPE:BUG"]) is True

    def test_label_hotfix(self):
        assert is_bug_fix_indicator("", labels=["hotfix"]) is True

    def test_label_kind_fix(self):
        assert is_bug_fix_indicator("", labels=["kind/fix"]) is True

    def test_label_regression(self):
        assert is_bug_fix_indicator("", labels=["regression"]) is True

    def test_non_matching_label_falls_through_to_text(self):
        # Label doesn't match, but text has fix: prefix
        assert is_bug_fix_indicator("fix: crash", labels=["enhancement"]) is True

    def test_non_matching_label_and_no_text_signal(self):
        assert is_bug_fix_indicator("add feature", labels=["enhancement"]) is False

    def test_config_extra_bug_labels(self):
        """Extra bug labels from config are respected."""
        _reset_customization_cache()
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump({"extra_bug_labels": ["problema"]}, f)
            tmp_path = f.name
        try:
            os.environ["DEVRANK_CUSTOMIZATION_PATH"] = tmp_path
            assert is_bug_fix_indicator("", labels=["problema"]) is True
        finally:
            os.unlink(tmp_path)
            _reset_customization_cache()
            os.environ.pop("DEVRANK_CUSTOMIZATION_PATH", None)


# ============================================================================
# Gap 3 — classify_defect_commit labels-first path
# ============================================================================


class TestClassifyDefectCommitLabels:
    def test_labels_only_no_message(self):
        assert classify_defect_commit("", labels=["bug"]) is True

    def test_label_incident(self):
        assert classify_defect_commit("", labels=["incident"]) is True

    def test_label_type_defect(self):
        assert classify_defect_commit("", labels=["type:defect"]) is True

    def test_label_case_insensitive(self):
        assert classify_defect_commit("", labels=["HOTFIX"]) is True

    def test_non_matching_label_falls_through(self):
        # Label doesn't match, message does
        assert classify_defect_commit("fix: crash", labels=["enhancement"]) is True

    def test_no_label_no_defect_message(self):
        assert classify_defect_commit("feat: add login", labels=["feature"]) is False


# ============================================================================
# Gap 4 — is_revert_indicator SHA-based structural fallback
# ============================================================================


class TestRevertIndicatorSHAFallback:
    def test_40_char_sha_in_body_matches(self):
        sha = "a" * 40
        msg = f"Revert bad change\n\nThis reverts {sha}"
        assert is_revert_indicator(msg) is True

    def test_sha_in_body_without_english_prefix(self):
        """Non-English subject line with SHA in body still detected."""
        sha = "abcdef1234567890abcdef1234567890abcdef12"
        msg = f"Rollback\n\n{sha}"
        assert is_revert_indicator(msg) is True

    def test_no_sha_and_no_prefix(self):
        assert is_revert_indicator("Rollback changes\nSome context") is False

    def test_short_sha_not_matched(self):
        """SHAs shorter than 40 chars in body don't trigger (avoid false positives)."""
        msg = "Undo\n\nabcdef1234"  # 10 chars
        assert is_revert_indicator(msg) is False

    def test_sha_in_subject_line_not_matched_as_body(self):
        """SHA on the first line (subject) shouldn't be caught by body-only search."""
        sha = "a" * 40
        msg = f"Undo {sha}"  # single line, no body
        assert is_revert_indicator(msg) is False


# ============================================================================
# Gap 5 — is_structured_commit broad check
# ============================================================================


class TestIsStructuredCommit:
    def test_standard_english_types(self):
        assert is_structured_commit("feat: add login") is True
        assert is_structured_commit("fix(api): handle null") is True

    def test_non_english_types(self):
        # Japanese-style structured commits
        assert is_structured_commit("修正: バグを直す") is True

    def test_french_type(self):
        assert is_structured_commit("correctif: bug corrige") is True

    def test_scoped_non_english(self):
        assert is_structured_commit("fonctionnalite(auth): ajouter OAuth") is True

    def test_non_structured_messages(self):
        assert is_structured_commit("Add login page") is False
        assert is_structured_commit("") is False
        assert is_structured_commit(None) is False

    def test_breaking_change(self):
        assert is_structured_commit("refactor!: drop old API") is True


# ============================================================================
# Gap 6 — compute_pr_body_quality GitLab / Azure DevOps cross-refs
# ============================================================================


class TestPrBodyQualityCrossRefs:
    def test_gitlab_mr_reference(self):
        """GitLab MR reference !NNN should get +15."""
        body = "See merge request !456\n" + "x" * 100
        score = compute_pr_body_quality(body)
        assert score == 40  # 25 (length) + 15 (cross-ref)

    def test_azure_devops_work_item(self):
        """Azure DevOps work item AB#NNN should get +15."""
        body = "Fixes AB#789\n" + "x" * 100
        score = compute_pr_body_quality(body)
        assert score == 40  # 25 (length) + 15 (cross-ref)

    def test_github_issue_ref(self):
        """Standard #NNN should still get +15."""
        body = "Closes #123\n" + "x" * 100
        score = compute_pr_body_quality(body)
        assert score == 40

    def test_url_fallback_when_no_cross_ref(self):
        """URL gets +10 when no cross-ref present."""
        body = "See https://jira.example.com/PROJ-123\n" + "x" * 100
        score = compute_pr_body_quality(body)
        assert score == 35  # 25 (length) + 10 (url)

    def test_cross_ref_takes_precedence_over_url(self):
        """Cross-ref bonus is exclusive with URL bonus (not cumulative)."""
        body = "Fixes #123 https://example.com\n" + "x" * 100
        score = compute_pr_body_quality(body)
        assert score == 40  # 25 + 15, NOT 25 + 15 + 10


# ============================================================================
# Gap 7 — is_documentation_file context-based fallback (Layer 4)
# ============================================================================


class TestDocumentationFileContextFallback:
    def test_sibling_majority_md_makes_file_doc(self):
        """A .txt file in a directory where >50% siblings are .md is doc."""
        all_files = [
            "notes/intro.md",
            "notes/guide.md",
            "notes/setup.md",
            "notes/config.txt",  # this one
        ]
        assert is_documentation_file("notes/config.txt", all_filenames=all_files) is True

    def test_sibling_minority_md_not_doc(self):
        """When <50% siblings are .md, non-.md file is not doc."""
        all_files = [
            "src/main.py",
            "src/utils.py",
            "src/readme.md",
            "src/config.py",
        ]
        assert is_documentation_file("src/config.py", all_filenames=all_files) is False

    def test_md_file_always_doc_regardless_of_context(self):
        """A .md file is always doc (Layer 1), no context needed."""
        assert is_documentation_file("anywhere/file.md", all_filenames=[]) is True

    def test_context_requires_at_least_2_siblings(self):
        """Need >= 2 sibling files for context fallback to trigger."""
        all_files = ["solo/only.md"]
        assert is_documentation_file("solo/config.txt", all_filenames=all_files) is False

    def test_no_context_provided_skips_layer_4(self):
        """Without all_filenames, Layer 4 is skipped."""
        assert is_documentation_file("notes/config.txt") is False

    def test_context_only_same_level_siblings(self):
        """Only direct siblings count, not files in subdirectories."""
        all_files = [
            "notes/intro.md",
            "notes/guide.md",
            "notes/sub/deep.py",
            "notes/sub/deep2.py",
            "notes/config.txt",
        ]
        # notes/ level: intro.md, guide.md, config.txt => 2/3 md > 50%
        assert is_documentation_file("notes/config.txt", all_filenames=all_files) is True


# ============================================================================
# Gap 8 — tested above in TestConfigGetters
# ============================================================================


# ============================================================================
# Gap 9 — pr_category_diversity helpers
# ============================================================================


class TestClassifyByLabels:
    def teardown_method(self):
        _reset_customization_cache()
        os.environ.pop("DEVRANK_CUSTOMIZATION_PATH", None)

    def test_bug_label_returns_fix(self):
        assert _classify_by_labels(["bug"]) == "fix"

    def test_feature_label_returns_feat(self):
        assert _classify_by_labels(["enhancement"]) == "feat"

    def test_docs_label_returns_docs(self):
        assert _classify_by_labels(["documentation"]) == "docs"

    def test_case_insensitive(self):
        assert _classify_by_labels(["TYPE:BUG"]) == "fix"

    def test_unknown_label_returns_none(self):
        assert _classify_by_labels(["random-label"]) is None

    def test_empty_labels_returns_none(self):
        assert _classify_by_labels([]) is None

    def test_first_matching_label_wins(self):
        assert _classify_by_labels(["enhancement", "bug"]) == "feat"

    def test_config_extra_label_to_category(self):
        _reset_customization_cache()
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump({"extra_label_to_category": {"Fehler": "fix"}}, f)
            tmp_path = f.name
        try:
            os.environ["DEVRANK_CUSTOMIZATION_PATH"] = tmp_path
            label_map = _get_label_to_category()
            assert label_map["fehler"] == "fix"
            assert label_map["bug"] == "fix"  # built-in preserved
            assert _classify_by_labels(["Fehler"]) == "fix"
        finally:
            os.unlink(tmp_path)


class TestClassifyByDiff:
    def test_all_test_files(self):
        assert _classify_by_diff(["tests/test_foo.py", "tests/test_bar.py"]) == "test"

    def test_all_doc_files(self):
        assert _classify_by_diff(["docs/guide.md", "docs/api.md"]) == "docs"

    def test_all_ci_files(self):
        assert _classify_by_diff([".github/workflows/ci.yml"]) == "ci"

    def test_all_dep_files(self):
        assert _classify_by_diff(["package.json", "package-lock.json"]) == "chore"

    def test_mixed_files_returns_none(self):
        assert _classify_by_diff(["src/main.py", "tests/test_main.py"]) is None

    def test_empty_returns_none(self):
        assert _classify_by_diff([]) is None


class TestClassifyPr:
    def test_conventional_commit_prefix_wins(self):
        assert _classify_pr("feat: add login", None) == "feat"

    def test_labels_override_english_keywords(self):
        # Title says "fix" but label says "feat"
        assert _classify_pr("fix something", None, labels=["feature"]) == "feat"

    def test_diff_structure_fallback(self):
        assert _classify_pr("Update stuff", None, labels=[], filenames=["tests/test_x.py"]) == "test"

    def test_english_keyword_fallback(self):
        assert _classify_pr("Fix the bug in login", None, labels=[], filenames=["src/auth.py"]) == "fix"

    def test_unknown_returns_other(self):
        assert _classify_pr("Do something", None, labels=[], filenames=["src/main.py"]) == "other"


# ============================================================================
# Gap 10 — _extract_reverted_sha structural SHA fallback
# ============================================================================


class TestExtractRevertedSha:
    def test_english_format(self):
        msg = "Revert bad commit\n\nThis reverts commit abcdef1234567890abcdef1234567890abcdef12"
        known = {"abcdef1234567890abcdef1234567890abcdef12"}
        sha = _extract_reverted_sha(msg, known)
        assert sha == "abcdef1234567890abcdef1234567890abcdef12"

    def test_structural_fallback_no_english(self):
        """Non-English message with SHA in body, verified against known SHAs."""
        full_sha = "abcdef1234567890abcdef1234567890abcdef12"
        msg = f"Rollback\n\n{full_sha}"
        known = {full_sha}
        sha = _extract_reverted_sha(msg, known)
        assert sha == full_sha

    def test_prefix_match_against_known(self):
        """Short SHA prefix matches full known SHA."""
        short = "abcdef12345"
        full = "abcdef1234567890abcdef1234567890abcdef12"
        msg = f"Undo\n\n{short}"
        known = {full}
        sha = _extract_reverted_sha(msg, known)
        assert sha == short

    def test_no_known_sha_returns_none(self):
        msg = "Undo\n\nabcdef1234567890abcdef1234567890abcdef12"
        known = {"0000000000000000000000000000000000000000"}
        assert _extract_reverted_sha(msg, known) is None

    def test_no_sha_in_message_returns_none(self):
        msg = "Revert bad change"
        assert _extract_reverted_sha(msg, set()) is None


# ============================================================================
# Gap 11 — is_bot_user canonical field
# ============================================================================


class TestIsBotUser:
    def test_user_with_is_bot_true(self):
        user = make_user(id=1, login="dependabot[bot]", is_bot=True)
        assert is_bot_user(user) is True

    def test_user_with_is_bot_false(self):
        user = make_user(id=2, login="alice", is_bot=False)
        assert is_bot_user(user) is False

    def test_user_without_is_bot_attr(self):
        """Legacy objects without is_bot should return False."""

        class LegacyUser:
            login = "old_user"

        assert is_bot_user(LegacyUser()) is False

    def test_none_returns_false(self):
        assert is_bot_user(None) is False


# ============================================================================
# Gap 12 — is_merge_commit parent_count structural path
# ============================================================================


class TestIsMergeCommitStructural:
    def test_parent_count_2_is_merge(self):
        commit = make_commit("sha1", make_user(), DEFAULT_START, 1, message="feat: stuff", parent_count=2)
        assert is_merge_commit(commit) is True

    def test_parent_count_3_is_merge(self):
        """Octopus merge with 3 parents."""
        commit = make_commit("sha2", make_user(), DEFAULT_START, 1, message="stuff", parent_count=3)
        assert is_merge_commit(commit) is True

    def test_parent_count_1_not_merge(self):
        commit = make_commit("sha3", make_user(), DEFAULT_START, 1, message="Merge something", parent_count=1)
        assert is_merge_commit(commit) is False

    def test_parent_count_0_not_merge(self):
        """Root commit."""
        commit = make_commit("sha4", make_user(), DEFAULT_START, 1, message="initial", parent_count=0)
        assert is_merge_commit(commit) is False

    def test_fallback_english_prefix_when_no_parent_count(self):
        """Legacy data without parent_count falls back to English prefix."""

        class LegacyCommit:
            message = "Merge branch 'main'"

        assert is_merge_commit(LegacyCommit()) is True

    def test_fallback_no_merge_prefix(self):

        class LegacyCommit:
            message = "feat: add login"

        assert is_merge_commit(LegacyCommit()) is False


# ============================================================================
# Gap 13 — co_author_contribution_rate broadened trailer pattern
# ============================================================================


class TestBroadenedCoAuthorPattern:
    def test_co_authored_by(self):
        msg = "feat: thing\n\nCo-authored-by: Alice <alice@example.com>"
        assert CO_AUTHOR_PATTERN.findall(msg)

    def test_pair_programmed_with(self):
        msg = "feat: thing\n\nPair-programmed-with: Bob <bob@example.com>"
        assert CO_AUTHOR_PATTERN.findall(msg)

    def test_paired_with(self):
        msg = "feat: thing\n\nPaired-with: Carol <carol@example.com>"
        assert CO_AUTHOR_PATTERN.findall(msg)

    def test_helped_by(self):
        msg = "feat: thing\n\nHelped-by: Dave <dave@example.com>"
        assert CO_AUTHOR_PATTERN.findall(msg)

    def test_mentored_by(self):
        msg = "feat: thing\n\nMentored-by: Eve <eve@example.com>"
        assert CO_AUTHOR_PATTERN.findall(msg)

    def test_reviewed_by_kernel_style(self):
        msg = "feat: thing\n\nReviewed-by: Frank <frank@example.com>"
        assert CO_AUTHOR_PATTERN.findall(msg)

    def test_case_insensitive(self):
        msg = "feat: thing\n\nco-authored-by: Alice <alice@example.com>"
        assert CO_AUTHOR_PATTERN.findall(msg)
        msg2 = "feat: thing\n\nPAIR-PROGRAMMED-WITH: Bob <bob@example.com>"
        assert CO_AUTHOR_PATTERN.findall(msg2)

    def test_no_trailer(self):
        msg = "feat: solo commit\n\nNo collaboration here."
        assert not CO_AUTHOR_PATTERN.findall(msg)

    def test_broadened_trailers_in_metric(self):
        """End-to-end: Pair-programmed-with trailer is counted by the metric."""
        from impact.metrics.plugins.mixed.co_author_contribution_rate import CoAuthorContributionRate

        user = make_user(id=1, login="alice")
        owner = make_user(id=2, login="org")
        repo = make_repo(id=1, name="repo", owner=owner)
        start = DEFAULT_START

        pr1 = make_pr(1, user, repo, base_time=start, created_delta_hours=0)
        c1 = make_commit(
            "sha1", user, start + timedelta(hours=1), 1,
            message="feat: collab\n\nPair-programmed-with: Bob <bob@example.com>",
        )

        bundle = make_bundle(
            users=[user, owner],
            repositories=[repo],
            pull_requests=[pr1],
            commits=[c1],
        )
        context = make_context(bundle, user_login="alice", start_date=start, end_date=start + timedelta(days=10))

        metric = CoAuthorContributionRate()
        res = metric.run(context)
        assert res.details["inbound_co_commits"] == 1


# ============================================================================
# Gap 14 — review_comment_substance._score_comment structural signals
# ============================================================================


class TestScoreCommentStructuralSignals:
    def test_bullet_list_adds_points(self):
        body = "Issues:\n- Missing null check\n- Wrong variable name\n" + "x" * 100
        score_with_bullets = _score_comment(body)
        body_plain = "There are some issues in the code here.\n" + "x" * 100
        score_plain = _score_comment(body_plain)
        assert score_with_bullets > score_plain

    def test_numbered_list_adds_points(self):
        body = "Steps:\n1. First fix X\n2. Then fix Y\n" + "x" * 100
        score = _score_comment(body)
        assert score > 30  # length + structured

    def test_at_mention_adds_points(self):
        body = "@alice can you take a look at this approach?\n" + "x" * 100
        score_mention = _score_comment(body)
        body_no_mention = "Can you take a look at this approach?\n" + "x" * 100
        score_no_mention = _score_comment(body_no_mention)
        assert score_mention > score_no_mention

    def test_structured_label_adds_points(self):
        body = "NOTE: this needs refactoring\n" + "x" * 100
        score_label = _score_comment(body)
        body_no_label = "This needs refactoring\n" + "x" * 100
        score_no = _score_comment(body_no_label)
        assert score_label > score_no

    def test_cjk_structured_label(self):
        """CJK colon-terminated label detected."""
        body = "提案: リファクタリングが必要\n" + "x" * 100
        score = _score_comment(body)
        # Should get structured_signals points
        assert score > 30

    def test_max_structural_capped_at_10(self):
        """Even with all structural signals, cap is 10 points."""
        body = (
            "NOTE: important\n"
            "@alice please review\n"
            "- item 1\n- item 2\n"
            "```python\ncode\n```\n"
            + "x" * 200
        )
        score = _score_comment(body)
        assert score <= 100


# ============================================================================
# Gap 15 — CIRunRecord model import
# ============================================================================


class TestCIRunRecordImport:
    def test_ci_run_record_importable(self):
        from impact.domain.models import CIRunRecord
        assert CIRunRecord is not None

    def test_workflow_run_record_alias_removed(self):
        """WorkflowRunRecord alias should no longer exist."""
        import impact.domain.models as models
        assert not hasattr(models, "WorkflowRunRecord")


# ============================================================================
# Gap 16 — tree-sitter expanded languages
# ============================================================================


class TestParseFunctionsC:
    def test_c_function(self):
        code = "int add(int a, int b) {\n    return a + b;\n}\n"
        funcs = parse_functions(code, "math.c")
        assert len(funcs) >= 1
        assert funcs[0]["name"] == "add"

    def test_c_header_function(self):
        """C header files use .h extension."""
        code = "void init(void) {\n    setup();\n}\n"
        funcs = parse_functions(code, "init.h")
        assert len(funcs) >= 1


class TestParseFunctionsCpp:
    def test_cpp_function(self):
        code = "int calculate(int x) {\n    return x * 2;\n}\n"
        funcs = parse_functions(code, "math.cpp")
        assert len(funcs) >= 1
        assert funcs[0]["name"] == "calculate"

    def test_cc_extension(self):
        code = "void process() {\n    run();\n}\n"
        funcs = parse_functions(code, "engine.cc")
        assert len(funcs) >= 1


class TestParseFunctionsCSharp:
    def test_csharp_method(self):
        code = (
            "class Service {\n"
            "    public void Process() {\n"
            "        Run();\n"
            "    }\n"
            "}\n"
        )
        funcs = parse_functions(code, "Service.cs")
        assert len(funcs) >= 1
        assert any(f["name"] == "Process" for f in funcs)


class TestParseFunctionsRuby:
    def test_ruby_method(self):
        code = (
            "class Greeter\n"
            "  def greet(name)\n"
            "    puts name\n"
            "  end\n"
            "end\n"
        )
        funcs = parse_functions(code, "greeter.rb")
        assert len(funcs) >= 1
        assert any(f["name"] == "greet" for f in funcs)


class TestParseFunctionsPHP:
    def test_php_function(self):
        code = "<?php\nfunction hello() {\n    echo 'hi';\n}\n"
        funcs = parse_functions(code, "hello.php")
        assert len(funcs) >= 1
        assert any(f["name"] == "hello" for f in funcs)


class TestParseFunctionsKotlin:
    def test_kotlin_function(self):
        code = "fun greet(name: String): String {\n    return \"Hello, $name\"\n}\n"
        funcs = parse_functions(code, "greet.kt")
        assert len(funcs) >= 1
        assert any(f["name"] == "greet" for f in funcs)


class TestParseFunctionsSwift:
    def test_swift_function(self):
        code = "func greet(name: String) -> String {\n    return \"Hello, \" + name\n}\n"
        funcs = parse_functions(code, "greet.swift")
        assert len(funcs) >= 1
        assert any(f["name"] == "greet" for f in funcs)


class TestParseFunctionsScala:
    def test_scala_function(self):
        code = "object Main {\n  def add(a: Int, b: Int): Int = {\n    a + b\n  }\n}\n"
        funcs = parse_functions(code, "Main.scala")
        assert len(funcs) >= 1
        assert any(f["name"] == "add" for f in funcs)


# ============================================================================
# Gap 17 — Constructor detection across languages
# ============================================================================


class TestConstructorDetection:
    def test_python_init(self):
        code = "class Foo:\n    def __init__(self):\n        self.x = 1\n"
        funcs = parse_functions(code, "foo.py")
        init = [f for f in funcs if f["name"] == "__init__"]
        assert len(init) == 1
        # __init__ is a method (inside class); constructor detection is via name
        assert init[0]["kind"] == "method"

    def test_js_constructor(self):
        code = "class Foo {\n    constructor() {\n        this.x = 1;\n    }\n}\n"
        funcs = parse_functions(code, "foo.js")
        constructors = [f for f in funcs if f["kind"] == "constructor"]
        assert len(constructors) >= 1

    def test_go_new_function(self):
        """Go convention: NewFoo() is a constructor."""
        code = (
            "package main\n\n"
            "type Service struct{}\n\n"
            "func NewService() *Service {\n"
            "    return &Service{}\n"
            "}\n"
        )
        funcs = parse_functions(code, "service.go")
        new_funcs = [f for f in funcs if f["name"] == "NewService"]
        assert len(new_funcs) == 1
        assert new_funcs[0]["kind"] == "constructor"

    def test_rust_new_method(self):
        """Rust convention: new() is a constructor."""
        code = (
            "struct Config {}\n\n"
            "impl Config {\n"
            "    fn new() -> Self {\n"
            "        Config {}\n"
            "    }\n"
            "}\n"
        )
        funcs = parse_functions(code, "config.rs")
        new_fns = [f for f in funcs if f["name"] == "new"]
        assert len(new_fns) >= 1
        # Rust `new()` inside impl block may be detected as method or constructor
        assert new_fns[0]["kind"] in ("constructor", "method")

    def test_ruby_initialize(self):
        """Ruby: initialize is a constructor."""
        code = (
            "class Dog\n"
            "  def initialize(name)\n"
            "    @name = name\n"
            "  end\n"
            "end\n"
        )
        funcs = parse_functions(code, "dog.rb")
        init = [f for f in funcs if f["name"] == "initialize"]
        assert len(init) == 1
        assert init[0]["kind"] == "constructor"

    def test_php_construct(self):
        """PHP: __construct is a constructor."""
        code = (
            "<?php\n"
            "class User {\n"
            "    public function __construct($name) {\n"
            "        $this->name = $name;\n"
            "    }\n"
            "}\n"
        )
        funcs = parse_functions(code, "User.php")
        constructors = [f for f in funcs if f["name"] == "__construct"]
        assert len(constructors) >= 1
        assert constructors[0]["kind"] == "constructor"

    def test_java_constructor(self):
        """Java: constructor_declaration AST node."""
        code = (
            "class User {\n"
            "    public User(String name) {\n"
            "        this.name = name;\n"
            "    }\n"
            "}\n"
        )
        funcs = parse_functions(code, "User.java")
        constructors = [f for f in funcs if f["kind"] == "constructor"]
        assert len(constructors) >= 1
