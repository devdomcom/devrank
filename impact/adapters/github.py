import json
from pathlib import Path
from typing import Dict

from impact.adapters.base import ProviderAdapter
from impact.domain.models import (
    CanonicalBundle, User, Repository, Branch, PullRequest, Commit, ReviewRecord, CommentRecord,
    PullRequestState, ReviewState, CommentType, UserType
)


class GitHubAdapter(ProviderAdapter):
    def parse_dump(self, dump_path: str) -> CanonicalBundle:
        path = Path(dump_path)
        # Read manifest
        manifest_path = path / 'dump_manifest.json'
        with open(manifest_path, 'r') as f:
            manifest = json.load(f)
        user_login = manifest['user']

        users: Dict[int, User] = {}
        repos: Dict[int, Repository] = {}
        prs: list[PullRequest] = []
        commits: list[Commit] = []
        reviews: list[ReviewRecord] = []
        comments: list[CommentRecord] = []

        # Parse pull_requests.jsonl
        pr_file = path / "canonical" / "pull_requests.jsonl"
        if pr_file.exists():
            with open(pr_file, 'r') as f:
                for line in f:
                    pr_dict = json.loads(line.strip())
                    user_dict = pr_dict['user']
                    if user_dict['login'] == user_login:
                        # Create user
                        user_id = user_dict['id']
                        if user_id not in users:
                            users[user_id] = User(**user_dict)
                        user = users[user_id]

                        # Repo
                        repo_id = pr_dict['base']['repo']['id']
                        if repo_id not in repos:
                            repo_dict = pr_dict['base']['repo']
                            owner_dict = repo_dict['owner']
                            owner_id = owner_dict['id']
                            if owner_id not in users:
                                users[owner_id] = User(**owner_dict)
                            owner = users[owner_id]
                            repos[repo_id] = Repository(id=repo_id, name=repo_dict['name'], full_name=repo_dict['full_name'], owner=owner)
                        repo = repos[repo_id]

                        # Base and head branches
                        base_dict = pr_dict['base']
                        head_dict = pr_dict['head']
                        base_user_dict = base_dict['user']
                        base_user_id = base_user_dict['id']
                        if base_user_id not in users:
                            users[base_user_id] = User(**base_user_dict)
                        head_user_dict = head_dict['user']
                        head_user_id = head_user_dict['id']
                        if head_user_id not in users:
                            users[head_user_id] = User(**head_user_dict)
                        base = Branch(label=base_dict['label'], ref=base_dict['ref'], sha=base_dict['sha'], user=users[base_user_id], repo=repo)
                        head = Branch(label=head_dict['label'], ref=head_dict['ref'], sha=head_dict['sha'], user=users[head_user_id], repo=repo)

                        # merged_by
                        merged_by = None
                        if pr_dict.get('merged_by'):
                            mb_dict = pr_dict['merged_by']
                            mb_id = mb_dict['id']
                            if mb_id not in users:
                                users[mb_id] = User(**mb_dict)
                            merged_by = users[mb_id]

                        pr = PullRequest(
                            id=pr_dict['id'],
                            number=pr_dict['number'],
                            title=pr_dict['title'],
                            body=pr_dict.get('body'),
                            state=PullRequestState(pr_dict['state']),
                            user=user,
                            created_at=pr_dict['created_at'],
                            updated_at=pr_dict.get('updated_at'),
                            closed_at=pr_dict.get('closed_at'),
                            merged_at=pr_dict.get('merged_at'),
                            merged=pr_dict['merged'],
                            merge_commit_sha=pr_dict.get('merge_commit_sha'),
                            repository=repo,
                            base=base,
                            head=head,
                            commits=pr_dict['commits'],
                            additions=pr_dict['additions'],
                            deletions=pr_dict['deletions'],
                            changed_files=pr_dict['changed_files'],
                            merged_by=merged_by,
                            comments=pr_dict['comments'],
                            review_comments=pr_dict['review_comments']
                        )
                        prs.append(pr)

        user_pr_numbers = {pr.number for pr in prs}

        # Parse commits.jsonl
        commit_file = path / "canonical" / "commits.jsonl"
        if commit_file.exists():
            with open(commit_file, 'r') as f:
                for line in f:
                    commit_dict = json.loads(line.strip())
                    author_dict = commit_dict['author']
                    author_id = author_dict['id']
                    if author_id not in users:
                        users[author_id] = User(**author_dict)
                    committer_dict = commit_dict['committer']
                    committer_id = committer_dict['id']
                    if committer_id not in users:
                        users[committer_id] = User(**committer_dict)
                    if author_dict['login'] == user_login:
                        commit = Commit(
                            sha=commit_dict['sha'],
                            author=users[author_id],
                            committer=users[committer_id],
                            message=commit_dict['commit']['message'],
                            date=commit_dict['commit']['author']['date'],
                            pull_request_number=commit_dict.get('pull_request_number'),
                            idx=commit_dict.get('idx')
                        )
                        commits.append(commit)

        # Parse reviews.jsonl
        review_file = path / "canonical" / "reviews.jsonl"
        if review_file.exists():
            with open(review_file, 'r') as f:
                for line in f:
                    review_dict = json.loads(line.strip())
                    user_dict = review_dict['user']
                    pr_number = int(review_dict['pull_request_url'].split('/')[-1])
                    if user_dict['login'] == user_login or pr_number in user_pr_numbers:
                        user_id = user_dict['id']
                        if user_id not in users:
                            users[user_id] = User(**user_dict)
                        user = users[user_id]
                        review = ReviewRecord(
                            id=review_dict['id'],
                            user=user,
                            body=review_dict.get('body'),
                            state=ReviewState(review_dict['state']),
                            submitted_at=review_dict['submitted_at'],
                            pull_request_number=pr_number
                        )
                        reviews.append(review)

        # Parse review_comments.jsonl
        rc_file = path / "canonical" / "review_comments.jsonl"
        if rc_file.exists():
            with open(rc_file, 'r') as f:
                for line in f:
                    comment_dict = json.loads(line.strip())
                    user_dict = comment_dict['user']
                    pr_number = int(comment_dict['pull_request_url'].split('/')[-1])
                    if user_dict['login'] == user_login or pr_number in user_pr_numbers:
                        user_id = user_dict['id']
                        if user_id not in users:
                            users[user_id] = User(**user_dict)
                        user = users[user_id]
                        comment = CommentRecord(
                            id=comment_dict['id'],
                            user=user,
                            body=comment_dict['body'],
                            created_at=comment_dict['created_at'],
                            updated_at=comment_dict.get('updated_at'),
                            type=CommentType.REVIEW,
                            pull_request_number=pr_number,
                            review_id=comment_dict.get('pull_request_review_id'),
                            in_reply_to_id=comment_dict.get('in_reply_to_id'),
                            path=comment_dict.get('path'),
                            position=comment_dict.get('position')
                        )
                        comments.append(comment)

        # Parse issue_comments.jsonl
        ic_file = path / "canonical" / "issue_comments.jsonl"
        if ic_file.exists():
            with open(ic_file, 'r') as f:
                for line in f:
                    comment_dict = json.loads(line.strip())
                    user_dict = comment_dict['user']
                    issue_number = int(comment_dict['issue_url'].split('/')[-1])
                    if user_dict['login'] == user_login or issue_number in user_pr_numbers:
                        user_id = user_dict['id']
                        if user_id not in users:
                            users[user_id] = User(**user_dict)
                        user = users[user_id]
                        comment = CommentRecord(
                            id=comment_dict['id'],
                            user=user,
                            body=comment_dict['body'],
                            created_at=comment_dict['created_at'],
                            updated_at=comment_dict.get('updated_at'),
                            type=CommentType.ISSUE,
                            pull_request_number=issue_number,
                            review_id=None,
                            in_reply_to_id=None,
                            path=None,
                            position=None
                        )
                        comments.append(comment)

        return CanonicalBundle(
            users=list(users.values()),
            repositories=list(repos.values()),
            pull_requests=prs,
            commits=commits,
            reviews=reviews,
            comments=comments
        )