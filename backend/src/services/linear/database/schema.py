from __future__ import annotations

from datetime import date, datetime
from enum import Enum
from typing import Any, Optional

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    JSON,
    String,
    Table,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    """Base declarative class for Linear ORM models."""


class ProjectMilestoneStatus(str, Enum):
    """The status of a project milestone."""

    DONE = "done"
    NEXT = "next"
    OVERDUE = "overdue"
    UNSTARTED = "unstarted"


issue_label_issue_association = Table(
    "issue_label_issue_association",
    Base.metadata,
    Column("issue_id", ForeignKey("issues.id"), primary_key=True),
    Column("issue_label_id", ForeignKey("issue_labels.id"), primary_key=True),
)

issue_subscriber_user_association = Table(
    "issue_subscriber_user_association",
    Base.metadata,
    Column("issue_id", ForeignKey("issues.id"), primary_key=True),
    Column("user_id", ForeignKey("users.id"), primary_key=True),
)

team_project_association = Table(
    "team_project_association",
    Base.metadata,
    Column("team_id", ForeignKey("teams.id"), primary_key=True),
    Column("project_id", ForeignKey("projects.id"), primary_key=True),
)

initiative_project_association = Table(
    "initiative_project_association",
    Base.metadata,
    Column("initiative_id", ForeignKey("initiatives.id"), primary_key=True),
    Column("project_id", ForeignKey("projects.id"), primary_key=True),
)

project_label_project_association = Table(
    "project_label_project_association",
    Base.metadata,
    Column("project_id", ForeignKey("projects.id"), primary_key=True),
    Column("project_label_id", ForeignKey("project_labels.id"), primary_key=True),
)

project_members_association = Table(
    "project_members_association",
    Base.metadata,
    Column("project_id", ForeignKey("projects.id"), primary_key=True),
    Column("user_id", ForeignKey("users.id"), primary_key=True),
)

comment_subscribers_association = Table(
    "comment_subscribers_association",
    Base.metadata,
    Column("comment_id", ForeignKey("comments.id"), primary_key=True),
    Column("user_id", ForeignKey("users.id"), primary_key=True),
)

document_subscribers_association = Table(
    "document_subscribers_association",
    Base.metadata,
    Column("document_id", ForeignKey("documents.id"), primary_key=True),
    Column("user_id", ForeignKey("users.id"), primary_key=True),
)


class Issue(Base):
    __tablename__ = "issues"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    activitySummary: Mapped[Optional[dict[str, Any]]] = mapped_column(
        JSON, nullable=True
    )
    addedToCycleAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    addedToProjectAt: Mapped[Optional[datetime]] = mapped_column(
        DateTime, nullable=True
    )
    addedToTeamAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    archivedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    asksExternalUserRequesterId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("external_users.id"), nullable=True
    )
    asksExternalUserRequester: Mapped[Optional["ExternalUser"]] = relationship(
        "ExternalUser", foreign_keys=[asksExternalUserRequesterId]
    )
    asksRequesterId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("users.id"), nullable=True
    )
    asksRequester: Mapped[Optional["User"]] = relationship(
        "User",
        back_populates="asksRequestedIssues",
        foreign_keys="Issue.asksRequesterId",
    )
    assigneeId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("users.id"), nullable=True
    )
    assignee: Mapped[Optional["User"]] = relationship(
        "User", back_populates="assignedIssues", foreign_keys="Issue.assigneeId"
    )
    attachments: Mapped[list["Attachment"]] = relationship(
        "Attachment", back_populates="issue", foreign_keys="Attachment.issueId"
    )
    autoArchivedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    autoClosedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    autoClosedByParentClosing: Mapped[Optional[bool]] = mapped_column(
        Boolean, nullable=True
    )
    boardOrder: Mapped[float] = mapped_column(Float, nullable=False)
    # botActor skipped
    branchName: Mapped[str] = mapped_column(String, nullable=False)
    canceledAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    parentId: Mapped[Optional[str]] = mapped_column(
        String, ForeignKey("issues.id"), nullable=True
    )
    parent: Mapped[Optional["Issue"]] = relationship(
        "Issue", foreign_keys=[parentId], remote_side=[id], back_populates="children"
    )
    children: Mapped[list["Issue"]] = relationship(
        "Issue",
        foreign_keys=[parentId],
        back_populates="parent",
        cascade="all, delete-orphan",
        single_parent=True,
    )
    comments: Mapped[list["Comment"]] = relationship(
        "Comment", back_populates="issue", foreign_keys="Comment.issueId"
    )
    completedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    createdAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    creatorId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("users.id"), nullable=True
    )
    creator: Mapped[Optional["User"]] = relationship(
        "User", back_populates="createdIssues", foreign_keys="Issue.creatorId"
    )
    customerTicketCount: Mapped[int] = mapped_column(Integer, nullable=False)
    cycleId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("cycles.id"), nullable=True
    )
    cycle: Mapped[Optional["Cycle"]] = relationship(
        "Cycle", back_populates="issues", foreign_keys="Issue.cycleId"
    )
    delegateId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("users.id"), nullable=True
    )
    delegate: Mapped[Optional["User"]] = relationship(
        "User", back_populates="delegatedIssues", foreign_keys="Issue.delegateId"
    )
    description: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    descriptionData: Mapped[Optional[dict[str, Any]]] = mapped_column(
        JSON, nullable=True
    )
    descriptionState: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    documentContent: Mapped[Optional["DocumentContent"]] = relationship(
        "DocumentContent",
        back_populates="issue",
        foreign_keys="DocumentContent.issueId",
        uselist=False,
    )
    dueDate: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    estimate: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    externalUserCreatorId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("external_users.id"), nullable=True
    )
    externalUserCreator: Mapped[Optional["ExternalUser"]] = relationship(
        "ExternalUser", foreign_keys=[externalUserCreatorId]
    )
    favorite: Mapped[Optional["Favorite"]] = relationship(
        "Favorite",
        back_populates="issue",
        foreign_keys="Favorite.issueId",
        uselist=False,
    )
    formerAttachments: Mapped[list["Attachment"]] = relationship(
        "Attachment",
        back_populates="originalIssue",
        foreign_keys="Attachment.originalIssueId",
    )
    formerNeeds: Mapped[list["CustomerNeed"]] = relationship(
        "CustomerNeed",
        back_populates="originalIssue",
        foreign_keys="CustomerNeed.originalIssueId",
    )
    history: Mapped[list["IssueHistory"]] = relationship(
        "IssueHistory", back_populates="issue", foreign_keys="IssueHistory.issueId"
    )
    identifier: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    incomingSuggestions: Mapped[list["IssueSuggestion"]] = relationship(
        "IssueSuggestion",
        back_populates="issue",
        foreign_keys="IssueSuggestion.issueId",
    )
    integrationSourceType: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    inverseRelations: Mapped[list["IssueRelation"]] = relationship(
        "IssueRelation",
        back_populates="relatedIssue",
        foreign_keys="IssueRelation.relatedIssueId",
    )
    labelIds: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    labels: Mapped[list["IssueLabel"]] = relationship(
        "IssueLabel",
        secondary=issue_label_issue_association,
        back_populates="issues",
    )
    lastAppliedTemplateId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("templates.id"), nullable=True
    )
    lastAppliedTemplate: Mapped[Optional["Template"]] = relationship(
        "Template", foreign_keys=[lastAppliedTemplateId]
    )
    needs: Mapped[list["CustomerNeed"]] = relationship(
        "CustomerNeed", back_populates="issue", foreign_keys="CustomerNeed.issueId"
    )
    number: Mapped[float] = mapped_column(Float, nullable=False)
    previousIdentifiers: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    priority: Mapped[float] = mapped_column(Float, nullable=False)
    priorityLabel: Mapped[str] = mapped_column(String, nullable=False)
    prioritySortOrder: Mapped[float] = mapped_column(Float, nullable=False)
    projectId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("projects.id"), nullable=True
    )
    project: Mapped[Optional["Project"]] = relationship(
        "Project", back_populates="issues", foreign_keys="Issue.projectId"
    )
    convertedToProject: Mapped[Optional["Project"]] = relationship(
        "Project",
        back_populates="convertedFromIssue",
        foreign_keys="Project.convertedFromIssueId",
        uselist=False,
    )
    projectMilestoneId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("project_milestones.id"), nullable=True
    )
    projectMilestone: Mapped[Optional["ProjectMilestone"]] = relationship(
        "ProjectMilestone",
        back_populates="issues",
        foreign_keys="Issue.projectMilestoneId",
    )
    reactionData: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    reactions: Mapped[list["Reaction"]] = relationship(
        "Reaction", back_populates="issue", foreign_keys="Reaction.issueId"
    )
    relations: Mapped[list["IssueRelation"]] = relationship(
        "IssueRelation", back_populates="issue", foreign_keys="IssueRelation.issueId"
    )

    slaBreachesAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    slaHighRiskAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    slaMediumRiskAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    slaStartedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    # recurringIssueTemplate skipped
    slaType: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    snoozedById: Mapped[Optional[str]] = mapped_column(
        ForeignKey("users.id"), nullable=True
    )
    snoozedBy: Mapped[Optional["User"]] = relationship(
        "User", back_populates="snoozedIssues", foreign_keys="Issue.snoozedById"
    )
    snoozedUntilAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    sortOrder: Mapped[float] = mapped_column(Float, nullable=False)
    sourceCommentId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("comments.id"), nullable=True
    )
    sourceComment: Mapped[Optional["Comment"]] = relationship(
        "Comment",
        back_populates="sourceForIssues",
        foreign_keys="Issue.sourceCommentId",
    )
    startedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    startedTriageAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    stateId: Mapped[str] = mapped_column(
        ForeignKey("workflow_states.id"), nullable=False
    )
    state: Mapped["WorkflowState"] = relationship(
        "WorkflowState", back_populates="issues", foreign_keys="Issue.stateId"
    )
    subIssueSortOrder: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    subscribers: Mapped[list["User"]] = relationship(
        "User",
        secondary=issue_subscriber_user_association,
        back_populates="subscribedIssues",
    )
    suggestions: Mapped[list["IssueSuggestion"]] = relationship(
        "IssueSuggestion",
        back_populates="suggestedIssue",
        foreign_keys="IssueSuggestion.suggestedIssueId",
    )
    suggestionsGeneratedAt: Mapped[Optional[datetime]] = mapped_column(
        DateTime, nullable=True
    )
    # syncedWith skipped
    teamId: Mapped[str] = mapped_column(ForeignKey("teams.id"), nullable=False)
    team: Mapped["Team"] = relationship(
        "Team", back_populates="issues", foreign_keys="Issue.teamId"
    )
    title: Mapped[str] = mapped_column(String, nullable=False)
    trashed: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    triagedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    updatedAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    url: Mapped[str] = mapped_column(String, nullable=False)
    uncompletedInCycleUponCloseId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("cycles.id"), nullable=True
    )
    uncompletedInCycleUponClose: Mapped[Optional["Cycle"]] = relationship(
        "Cycle",
        back_populates="uncompletedIssuesUponClose",
        foreign_keys=[uncompletedInCycleUponCloseId],
    )
    reminderAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)


class Attachment(Base):
    __tablename__ = "attachments"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    issueId: Mapped[str] = mapped_column(ForeignKey("issues.id"), nullable=False)
    issue: Mapped[Issue] = relationship(
        "Issue", back_populates="attachments", foreign_keys="Attachment.issueId"
    )
    originalIssueId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("issues.id"), nullable=True
    )
    originalIssue: Mapped[Optional["Issue"]] = relationship(
        "Issue",
        back_populates="formerAttachments",
        foreign_keys="Attachment.originalIssueId",
    )
    archivedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    bodyData: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    createdAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    creatorId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("users.id"), nullable=True
    )
    creator: Mapped[Optional["User"]] = relationship("User", foreign_keys=[creatorId])
    externalUserCreatorId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("external_users.id"), nullable=True
    )
    externalUserCreator: Mapped[Optional["ExternalUser"]] = relationship(
        "ExternalUser", foreign_keys=[externalUserCreatorId]
    )
    groupBySource: Mapped[bool] = mapped_column(Boolean, nullable=False)
    metadata_: Mapped[dict[str, Any]] = mapped_column("metadata", JSON, nullable=False)
    source: Mapped[Optional[dict[str, Any]]] = mapped_column(JSON, nullable=True)
    sourceType: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    subtitle: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    title: Mapped[str] = mapped_column(String, nullable=False)
    updatedAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    url: Mapped[str] = mapped_column(String, nullable=False)
    iconUrl: Mapped[Optional[str]] = mapped_column(String, nullable=True)


class Comment(Base):
    __tablename__ = "comments"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    issue: Mapped[Optional["Issue"]] = relationship(
        "Issue", back_populates="comments", foreign_keys="Comment.issueId"
    )
    sourceForIssues: Mapped[list["Issue"]] = relationship(
        "Issue", back_populates="sourceComment", foreign_keys="Issue.sourceCommentId"
    )
    parent: Mapped[Optional["Comment"]] = relationship(
        "Comment",
        foreign_keys=lambda: Comment.parentId,
        remote_side=lambda: Comment.id,
        back_populates="children",
    )
    children: Mapped[list["Comment"]] = relationship(
        "Comment",
        foreign_keys=lambda: Comment.parentId,
        back_populates="parent",
        cascade="all, delete-orphan",
    )
    agentSessionId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("agent_sessions.id"), nullable=True
    )
    agentSession: Mapped[Optional["AgentSession"]] = relationship(
        "AgentSession",
        back_populates="commentWithActiveSession",
        foreign_keys=[agentSessionId],
        uselist=False,
    )
    agentSessions: Mapped[list["AgentSession"]] = relationship(
        "AgentSession", back_populates="comment", foreign_keys="AgentSession.commentId"
    )
    archivedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    body: Mapped[str] = mapped_column(String, nullable=False)
    bodyData: Mapped[str] = mapped_column(String, nullable=False)
    # botActor skipped
    createdAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    documentContentId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("document_contents.id"), nullable=True
    )
    documentContent: Mapped[Optional["DocumentContent"]] = relationship(
        "DocumentContent", foreign_keys=[documentContentId]
    )
    documentId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("documents.id"), nullable=True
    )
    editedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    # externalThread skipped
    externalUserId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("external_users.id"), nullable=True
    )
    externalUser: Mapped[Optional["ExternalUser"]] = relationship(
        "ExternalUser", foreign_keys=[externalUserId]
    )
    initiativeUpdate: Mapped[Optional["InitiativeUpdate"]] = relationship(
        "InitiativeUpdate",
        back_populates="comments",
        foreign_keys="Comment.initiativeUpdateId",
    )
    initiativeUpdateId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("initiative_updates.id"), nullable=True
    )
    issueId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("issues.id"), nullable=True
    )
    parentId: Mapped[Optional[str]] = mapped_column(
        String, ForeignKey("comments.id"), nullable=True
    )
    postId: Mapped[Optional[str]] = mapped_column(ForeignKey("posts.id"), nullable=True)
    post: Mapped[Optional["Post"]] = relationship("Post", foreign_keys=[postId])
    projectId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("projects.id"), nullable=True
    )
    projectUpdateId: Mapped[Optional[str]] = mapped_column(
        String,
        ForeignKey("project_updates.id"),
        nullable=True,
    )
    projectUpdate: Mapped[Optional["ProjectUpdate"]] = relationship(
        "ProjectUpdate", back_populates="comments", foreign_keys=[projectUpdateId]
    )
    quotedText: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    reactionData: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    reactions: Mapped[list["Reaction"]] = relationship(
        "Reaction", back_populates="comment", foreign_keys="Reaction.commentId"
    )
    resolvedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    resolvingComment: Mapped[Optional["Comment"]] = relationship(
        "Comment",
        foreign_keys=lambda: Comment.resolvingCommentId,
        remote_side=lambda: Comment.id,
        back_populates="resolvedComments",
        uselist=False,
    )
    resolvedComments: Mapped[list["Comment"]] = relationship(
        "Comment",
        foreign_keys=lambda: Comment.resolvingCommentId,
        back_populates="resolvingComment",
    )
    resolvingCommentId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("comments.id"), nullable=True
    )
    resolvingUserId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("users.id"), nullable=True
    )
    resolvingUser: Mapped[Optional["User"]] = relationship(
        "User", foreign_keys=[resolvingUserId]
    )
    # syncedWith skipped
    threadSummary: Mapped[Optional[dict[str, Any]]] = mapped_column(JSON, nullable=True)
    updatedAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    url: Mapped[str] = mapped_column(String, nullable=False)
    userId: Mapped[Optional[str]] = mapped_column(ForeignKey("users.id"), nullable=True)
    user: Mapped[Optional["User"]] = relationship("User", foreign_keys=[userId])
    subscribers: Mapped[list["User"]] = relationship(
        "User", secondary=comment_subscribers_association
    )
    document: Mapped[Optional["Document"]] = relationship(
        "Document", foreign_keys=[documentId]
    )


class InitiativeUpdate(Base):
    __tablename__ = "initiative_updates"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    comments: Mapped[list["Comment"]] = relationship(
        "Comment",
        back_populates="initiativeUpdate",
        foreign_keys="Comment.initiativeUpdateId",
    )
    initiativeId: Mapped[str] = mapped_column(
        ForeignKey("initiatives.id"), nullable=False
    )
    initiative: Mapped["Initiative"] = relationship(
        "Initiative", back_populates="updates", foreign_keys=[initiativeId]
    )
    lastUpdateForInitiative: Mapped[Optional["Initiative"]] = relationship(
        "Initiative",
        back_populates="lastUpdate",
        foreign_keys="Initiative.lastUpdateId",
        uselist=False,
    )


class AgentSession(Base):
    __tablename__ = "agent_sessions"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    commentId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("comments.id"), nullable=True
    )
    comment: Mapped[Optional["Comment"]] = relationship(
        "Comment", back_populates="agentSessions", foreign_keys=[commentId]
    )
    commentWithActiveSession: Mapped[Optional["Comment"]] = relationship(
        "Comment",
        back_populates="agentSession",
        foreign_keys="Comment.agentSessionId",
        uselist=False,
    )


class CustomerNeed(Base):
    __tablename__ = "customer_needs"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    originalIssueId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("issues.id"), nullable=True
    )
    originalIssue: Mapped[Optional["Issue"]] = relationship(
        "Issue",
        back_populates="formerNeeds",
        foreign_keys="CustomerNeed.originalIssueId",
    )
    issueId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("issues.id"), nullable=True
    )
    issue: Mapped[Optional["Issue"]] = relationship(
        "Issue", back_populates="needs", foreign_keys="CustomerNeed.issueId"
    )
    projectId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("projects.id"), nullable=True
    )
    project: Mapped[Optional["Project"]] = relationship(
        "Project", back_populates="needs", foreign_keys="CustomerNeed.projectId"
    )


class Cycle(Base):
    __tablename__ = "cycles"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    issues: Mapped[list["Issue"]] = relationship(
        "Issue", back_populates="cycle", foreign_keys="Issue.cycleId"
    )
    archivedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    autoArchivedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    completedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    completedIssueCountHistory: Mapped[list[float]] = mapped_column(
        JSON, nullable=False
    )
    completedScopeHistory: Mapped[list[float]] = mapped_column(JSON, nullable=False)
    createdAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    currentProgress: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    endsAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    inProgressScopeHistory: Mapped[list[float]] = mapped_column(JSON, nullable=False)
    inheritedFromId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("cycles.id"), nullable=True
    )
    inheritedFrom: Mapped[Optional["Cycle"]] = relationship(
        "Cycle",
        back_populates="inheritedChildren",
        foreign_keys=[inheritedFromId],
        remote_side=[id],
    )
    inheritedChildren: Mapped[list["Cycle"]] = relationship(
        "Cycle",
        back_populates="inheritedFrom",
        foreign_keys="Cycle.inheritedFromId",
    )
    isActive: Mapped[bool] = mapped_column(Boolean, nullable=False)
    isFuture: Mapped[bool] = mapped_column(Boolean, nullable=False)
    isNext: Mapped[bool] = mapped_column(Boolean, nullable=False)
    isPast: Mapped[bool] = mapped_column(Boolean, nullable=False)
    isPrevious: Mapped[bool] = mapped_column(Boolean, nullable=False)
    issueCountHistory: Mapped[list[float]] = mapped_column(JSON, nullable=False)
    name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    number: Mapped[float] = mapped_column(Float, nullable=False)
    progress: Mapped[float] = mapped_column(Float, nullable=False)
    progressHistory: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    scopeHistory: Mapped[list[float]] = mapped_column(JSON, nullable=False)
    startsAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    teamId: Mapped[str] = mapped_column(ForeignKey("teams.id"), nullable=False)
    team: Mapped["Team"] = relationship(
        "Team", back_populates="cycles", foreign_keys="Cycle.teamId"
    )
    uncompletedIssuesUponClose: Mapped[list["Issue"]] = relationship(
        "Issue",
        back_populates="uncompletedInCycleUponClose",
        foreign_keys="Issue.uncompletedInCycleUponCloseId",
    )
    updatedAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    activeForTeam: Mapped[Optional["Team"]] = relationship(
        "Team",
        back_populates="activeCycle",
        foreign_keys="Team.activeCycleId",
        uselist=False,
    )


class DocumentContent(Base):
    __tablename__ = "document_contents"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    issueId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("issues.id"), nullable=True
    )
    issue: Mapped[Optional["Issue"]] = relationship(
        "Issue", back_populates="documentContent", foreign_keys=[issueId]
    )
    projectId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("projects.id"), nullable=True
    )
    project: Mapped[Optional["Project"]] = relationship(
        "Project", back_populates="documentContent", foreign_keys=[projectId]
    )
    initiativeId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("initiatives.id"), nullable=True
    )
    initiative: Mapped[Optional["Initiative"]] = relationship(
        "Initiative",
        back_populates="documentContent",
        foreign_keys=[initiativeId],
        uselist=False,
    )
    archivedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    content: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    contentState: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    createdAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    documentId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("documents.id"), nullable=True
    )
    document: Mapped[Optional["Document"]] = relationship(
        "Document", foreign_keys=[documentId]
    )
    projectMilestoneId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("project_milestones.id"), nullable=True
    )
    projectMilestone: Mapped[Optional["ProjectMilestone"]] = relationship(
        "ProjectMilestone",
        back_populates="documentContent",
        foreign_keys=[projectMilestoneId],
        uselist=False,
    )
    restoredAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    updatedAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)


class Favorite(Base):
    __tablename__ = "favorites"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    issueId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("issues.id"), nullable=True
    )
    issue: Mapped[Optional["Issue"]] = relationship(
        "Issue", back_populates="favorite", foreign_keys=[issueId]
    )
    projectId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("projects.id"), nullable=True
    )
    project: Mapped[Optional["Project"]] = relationship(
        "Project", back_populates="favorite", foreign_keys=[projectId], uselist=False
    )


class IssueHistory(Base):
    __tablename__ = "issue_histories"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    issueId: Mapped[str] = mapped_column(ForeignKey("issues.id"), nullable=False)
    issue: Mapped[Issue] = relationship(
        "Issue", back_populates="history", foreign_keys=[issueId]
    )
    fromParentId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("issues.id"), nullable=True
    )
    fromParent: Mapped[Optional["Issue"]] = relationship(
        "Issue", foreign_keys=[fromParentId]
    )


class IssueSuggestion(Base):
    __tablename__ = "issue_suggestions"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    issueId: Mapped[str] = mapped_column(ForeignKey("issues.id"), nullable=False)
    issue: Mapped["Issue"] = relationship(
        "Issue", back_populates="incomingSuggestions", foreign_keys=[issueId]
    )
    suggestedIssueId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("issues.id"), nullable=True
    )
    suggestedIssue: Mapped[Optional["Issue"]] = relationship(
        "Issue", back_populates="suggestions", foreign_keys=[suggestedIssueId]
    )


class IssueRelation(Base):
    __tablename__ = "issue_relations"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    archivedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    createdAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    issueId: Mapped[str] = mapped_column(ForeignKey("issues.id"), nullable=False)
    relatedIssueId: Mapped[str] = mapped_column(ForeignKey("issues.id"), nullable=False)
    type: Mapped[str] = mapped_column(String, nullable=False)
    updatedAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)

    issue: Mapped[Issue] = relationship(
        "Issue", back_populates="relations", foreign_keys=[issueId]
    )
    relatedIssue: Mapped[Issue] = relationship(
        "Issue", back_populates="inverseRelations", foreign_keys=[relatedIssueId]
    )


class IssueLabel(Base):
    __tablename__ = "issue_labels"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    issues: Mapped[list["Issue"]] = relationship(
        "Issue",
        secondary=issue_label_issue_association,
        back_populates="labels",
    )
    teamId: Mapped[Optional[str]] = mapped_column(ForeignKey("teams.id"), nullable=True)
    team: Mapped[Optional["Team"]] = relationship(
        "Team", back_populates="labels", foreign_keys="IssueLabel.teamId"
    )
    organizationId: Mapped[str] = mapped_column(
        ForeignKey("organizations.id"), nullable=False
    )
    organization: Mapped["Organization"] = relationship(
        "Organization",
        back_populates="labels",
        foreign_keys="IssueLabel.organizationId",
    )
    archivedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    parentId: Mapped[Optional[str]] = mapped_column(
        String, ForeignKey("issue_labels.id"), nullable=True
    )
    parent: Mapped[Optional["IssueLabel"]] = relationship(
        "IssueLabel",
        foreign_keys=[parentId],
        remote_side=[id],
        back_populates="children",
    )
    children: Mapped[list["IssueLabel"]] = relationship(
        "IssueLabel",
        foreign_keys=[parentId],
        back_populates="parent",
        cascade="all, delete-orphan",
        single_parent=True,
    )
    color: Mapped[str] = mapped_column(String, nullable=False)
    createdAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    creatorId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("users.id"), nullable=True
    )
    creator: Mapped[Optional["User"]] = relationship("User", foreign_keys=[creatorId])
    description: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    inheritedFromId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("issue_labels.id"), nullable=True
    )
    inheritedFrom: Mapped[Optional["IssueLabel"]] = relationship(
        "IssueLabel",
        back_populates="inheritedChildren",
        foreign_keys=[inheritedFromId],
        remote_side=[id],
    )
    inheritedChildren: Mapped[list["IssueLabel"]] = relationship(
        "IssueLabel",
        back_populates="inheritedFrom",
        foreign_keys="IssueLabel.inheritedFromId",
    )
    isGroup: Mapped[bool] = mapped_column(Boolean, nullable=False)
    lastAppliedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    retiredAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    updatedAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)


class Project(Base):
    __tablename__ = "projects"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    issues: Mapped[list["Issue"]] = relationship(
        "Issue", back_populates="project", foreign_keys="Issue.projectId"
    )
    teams: Mapped[list["Team"]] = relationship(
        "Team",
        secondary=team_project_association,
        back_populates="projects",
    )
    archivedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    autoArchivedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    canceledAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    color: Mapped[str] = mapped_column(String, nullable=False)
    comments: Mapped[list["Comment"]] = relationship(
        "Comment", foreign_keys="Comment.projectId"
    )
    completedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    completedIssueCountHistory: Mapped[list[float]] = mapped_column(
        JSON, nullable=False
    )
    completedScopeHistory: Mapped[list[float]] = mapped_column(JSON, nullable=False)
    content: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    contentState: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    convertedFromIssueId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("issues.id"), nullable=True
    )
    convertedFromIssue: Mapped[Optional["Issue"]] = relationship(
        "Issue",
        back_populates="convertedToProject",
        foreign_keys=[convertedFromIssueId],
        uselist=False,
    )
    createdAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    creatorId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("users.id"), nullable=True
    )
    creator: Mapped[Optional["User"]] = relationship("User", foreign_keys=[creatorId])
    currentProgress: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    description: Mapped[str] = mapped_column(String, nullable=False)
    documentContent: Mapped[Optional["DocumentContent"]] = relationship(
        "DocumentContent",
        back_populates="project",
        foreign_keys="DocumentContent.projectId",
        uselist=False,
    )
    documents: Mapped[list["Document"]] = relationship(
        "Document", back_populates="project", foreign_keys="Document.projectId"
    )
    facets: Mapped[list["Facet"]] = relationship(
        "Facet", back_populates="sourceProject", foreign_keys="Facet.sourceProjectId"
    )
    # externalLinks skipped
    favorite: Mapped[Optional["Favorite"]] = relationship(
        "Favorite",
        back_populates="project",
        foreign_keys="Favorite.projectId",
        uselist=False,
    )
    frequencyResolution: Mapped[str] = mapped_column(String, nullable=False)
    health: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    healthUpdatedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    history: Mapped[list["ProjectHistory"]] = relationship(
        "ProjectHistory",
        back_populates="project",
        foreign_keys="ProjectHistory.projectId",
    )
    icon: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    inProgressScopeHistory: Mapped[list[float]] = mapped_column(JSON, nullable=False)
    initiatives: Mapped[list["Initiative"]] = relationship(
        "Initiative",
        secondary=initiative_project_association,
        back_populates="projects",
    )
    integrationsSettings: Mapped[Optional["IntegrationsSettings"]] = relationship(
        "IntegrationsSettings",
        back_populates="project",
        foreign_keys="IntegrationsSettings.projectId",
        uselist=False,
    )
    inverseRelations: Mapped[list["ProjectRelation"]] = relationship(
        "ProjectRelation",
        back_populates="project",
        foreign_keys="ProjectRelation.projectId",
    )
    issueCountHistory: Mapped[list[float]] = mapped_column(JSON, nullable=False)
    labelIds: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    labels: Mapped[list["ProjectLabel"]] = relationship(
        "ProjectLabel",
        secondary=project_label_project_association,
        back_populates="projects",
    )
    lastAppliedTemplateId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("templates.id"), nullable=True
    )
    lastAppliedTemplate: Mapped[Optional["Template"]] = relationship(
        "Template", foreign_keys=[lastAppliedTemplateId]
    )
    lastUpdateId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("project_updates.id"), nullable=True
    )
    lastUpdate: Mapped[Optional["ProjectUpdate"]] = relationship(
        "ProjectUpdate",
        back_populates="lastUpdateForProject",
        foreign_keys=[lastUpdateId],
        uselist=False,
    )
    updates: Mapped[list["ProjectUpdate"]] = relationship(
        "ProjectUpdate",
        back_populates="project",
        foreign_keys="ProjectUpdate.projectId",
    )
    leadId: Mapped[Optional[str]] = mapped_column(ForeignKey("users.id"), nullable=True)
    lead: Mapped[Optional["User"]] = relationship("User", foreign_keys=[leadId])
    members: Mapped[list["User"]] = relationship(
        "User", secondary=project_members_association
    )
    name: Mapped[str] = mapped_column(String, nullable=False)
    needs: Mapped[list["CustomerNeed"]] = relationship(
        "CustomerNeed", back_populates="project", foreign_keys="CustomerNeed.projectId"
    )
    priority: Mapped[int] = mapped_column(Integer, nullable=False)
    priorityLabel: Mapped[str] = mapped_column(String, nullable=False)
    prioritySortOrder: Mapped[float] = mapped_column(Float, nullable=False)
    progress: Mapped[float] = mapped_column(Float, nullable=False)
    progressHistory: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    projectMilestones: Mapped[list["ProjectMilestone"]] = relationship(
        "ProjectMilestone",
        back_populates="project",
        foreign_keys="ProjectMilestone.projectId",
    )
    projectUpdateRemindersPausedUntilAt: Mapped[Optional[datetime]] = mapped_column(
        DateTime, nullable=True
    )
    relations: Mapped[list["ProjectRelation"]] = relationship(
        "ProjectRelation",
        back_populates="relatedProject",
        foreign_keys="ProjectRelation.relatedProjectId",
    )
    scope: Mapped[float] = mapped_column(Float, nullable=False)
    scopeHistory: Mapped[list[float]] = mapped_column(JSON, nullable=False)
    slackIssueComments: Mapped[bool] = mapped_column(Boolean, nullable=False)
    slackIssueStatuses: Mapped[bool] = mapped_column(Boolean, nullable=False)
    slackNewIssue: Mapped[bool] = mapped_column(Boolean, nullable=False)
    slugId: Mapped[str] = mapped_column(String, nullable=False)
    sortOrder: Mapped[float] = mapped_column(Float, nullable=False)
    startDate: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    startDateResolution: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    startedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    state: Mapped[str] = mapped_column(String, nullable=False)
    statusId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("project_statuses.id"), nullable=True
    )
    status: Mapped[Optional["ProjectStatus"]] = relationship(
        "ProjectStatus", foreign_keys=[statusId]
    )
    targetDate: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    targetDateResolution: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    trashed: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    updateReminderFrequency: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True
    )
    updateReminderFrequencyInWeeks: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True
    )
    updateRemindersDay: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    updateRemindersHour: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    updatedAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    url: Mapped[str] = mapped_column(String, nullable=False)


class ProjectUpdate(Base):
    __tablename__ = "project_updates"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    comments: Mapped[list["Comment"]] = relationship(
        "Comment",
        back_populates="projectUpdate",
        foreign_keys="Comment.projectUpdateId",
    )
    projectId: Mapped[str] = mapped_column(ForeignKey("projects.id"), nullable=False)
    project: Mapped["Project"] = relationship(
        "Project", back_populates="updates", foreign_keys=[projectId]
    )
    lastUpdateForProject: Mapped[Optional["Project"]] = relationship(
        "Project",
        back_populates="lastUpdate",
        foreign_keys="Project.lastUpdateId",
        uselist=False,
    )


class ProjectMilestone(Base):
    __tablename__ = "project_milestones"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    issues: Mapped[list["Issue"]] = relationship(
        "Issue",
        back_populates="projectMilestone",
        foreign_keys="Issue.projectMilestoneId",
    )
    projectId: Mapped[str] = mapped_column(ForeignKey("projects.id"), nullable=False)
    project: Mapped["Project"] = relationship(
        "Project", back_populates="projectMilestones", foreign_keys=[projectId]
    )
    documentContent: Mapped[Optional["DocumentContent"]] = relationship(
        "DocumentContent",
        back_populates="projectMilestone",
        foreign_keys="DocumentContent.projectMilestoneId",
        uselist=False,
    )
    archivedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    createdAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    currentProgress: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    descriptionData: Mapped[Optional[dict[str, Any]]] = mapped_column(
        JSON, nullable=True
    )
    descriptionState: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    progress: Mapped[float] = mapped_column(Float, nullable=False)
    progressHistory: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    sortOrder: Mapped[float] = mapped_column(Float, nullable=False)
    status: Mapped[ProjectMilestoneStatus] = mapped_column(String, nullable=False)
    targetDate: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    updatedAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)


class Reaction(Base):
    __tablename__ = "reactions"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    issueId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("issues.id"), nullable=True
    )
    issue: Mapped[Optional["Issue"]] = relationship(
        "Issue", back_populates="reactions", foreign_keys=[issueId]
    )
    commentId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("comments.id"), nullable=True
    )
    comment: Mapped[Optional["Comment"]] = relationship(
        "Comment", back_populates="reactions", foreign_keys=[commentId]
    )


class Team(Base):
    __tablename__ = "teams"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    __table_args__ = (
        UniqueConstraint("organizationId", "key", name="uq_team_org_key"),
    )
    parentId: Mapped[Optional[str]] = mapped_column(
        String, ForeignKey("teams.id"), nullable=True
    )
    parent: Mapped[Optional["Team"]] = relationship(
        "Team", foreign_keys=[parentId], remote_side=[id], back_populates="children"
    )
    children: Mapped[list["Team"]] = relationship(
        "Team",
        foreign_keys=[parentId],
        back_populates="parent",
        cascade="all, delete-orphan",
        single_parent=True,
    )
    issues: Mapped[list["Issue"]] = relationship(
        "Issue", back_populates="team", foreign_keys="Issue.teamId"
    )
    teamMemberships: Mapped[list["TeamMembership"]] = relationship(
        "TeamMembership",
        back_populates="team",
        foreign_keys="TeamMembership.teamId",
    )
    members: Mapped[list["User"]] = relationship(
        "User",
        secondary="team_memberships",
        back_populates="teams",
        viewonly=True,
    )
    cycles: Mapped[list["Cycle"]] = relationship(
        "Cycle", back_populates="team", foreign_keys="Cycle.teamId"
    )
    activeCycleId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("cycles.id"), nullable=True
    )
    activeCycle: Mapped[Optional["Cycle"]] = relationship(
        "Cycle",
        back_populates="activeForTeam",
        foreign_keys=[activeCycleId],
        uselist=False,
    )
    aiThreadSummariesEnabled: Mapped[bool] = mapped_column(Boolean, nullable=False)
    archivedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    autoArchivePeriod: Mapped[float] = mapped_column(Float, nullable=False)
    autoCloseChildIssues: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    autoCloseParentIssues: Mapped[Optional[bool]] = mapped_column(
        Boolean, nullable=True
    )
    autoClosePeriod: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    autoCloseStateId: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    color: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    createdAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    currentProgress: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    cycleCalenderUrl: Mapped[str] = mapped_column(String, nullable=False)
    cycleCooldownTime: Mapped[float] = mapped_column(Float, nullable=False)
    cycleDuration: Mapped[float] = mapped_column(Float, nullable=False)
    cycleIssueAutoAssignCompleted: Mapped[bool] = mapped_column(Boolean, nullable=False)
    cycleIssueAutoAssignStarted: Mapped[bool] = mapped_column(Boolean, nullable=False)
    cycleLockToActive: Mapped[bool] = mapped_column(Boolean, nullable=False)
    cycleStartDay: Mapped[float] = mapped_column(Float, nullable=False)
    cyclesEnabled: Mapped[bool] = mapped_column(Boolean, nullable=False)
    defaultIssueEstimate: Mapped[float] = mapped_column(Float, nullable=False)
    workflowStates: Mapped[list["WorkflowState"]] = relationship(
        "WorkflowState",
        back_populates="team",
        foreign_keys="WorkflowState.teamId",
    )
    defaultIssueStateId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("workflow_states.id"), nullable=True
    )
    defaultIssueState: Mapped[Optional["WorkflowState"]] = relationship(
        "WorkflowState",
        back_populates="defaultForTeam",
        foreign_keys=[defaultIssueStateId],
        uselist=False,
    )
    defaultProjectTemplateId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("templates.id"), nullable=True
    )
    defaultProjectTemplate: Mapped[Optional["Template"]] = relationship(
        "Template",
        back_populates="defaultForTeam",
        foreign_keys=[defaultProjectTemplateId],
        uselist=False,
    )
    defaultTemplateForMembersId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("templates.id"), nullable=True
    )
    defaultTemplateForMembers: Mapped[Optional["Template"]] = relationship(
        "Template",
        back_populates="defaultForMembersTeam",
        foreign_keys=[defaultTemplateForMembersId],
        uselist=False,
    )
    defaultTemplateForNonMembersId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("templates.id"), nullable=True
    )
    defaultTemplateForNonMembers: Mapped[Optional["Template"]] = relationship(
        "Template",
        back_populates="defaultForNonMembersTeam",
        foreign_keys=[defaultTemplateForNonMembersId],
        uselist=False,
    )
    description: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    displayName: Mapped[str] = mapped_column(String, nullable=False)
    draftWorkflowStateId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("workflow_states.id"), nullable=True
    )
    draftWorkflowState: Mapped[Optional["WorkflowState"]] = relationship(
        "WorkflowState",
        back_populates="draftForTeam",
        foreign_keys=[draftWorkflowStateId],
        uselist=False,
    )
    facets: Mapped[list["Facet"]] = relationship(
        "Facet",
        back_populates="sourceTeam",
        foreign_keys="Facet.sourceTeamId",
    )
    gitAutomationStates: Mapped[list["GitAutomationState"]] = relationship(
        "GitAutomationState",
        back_populates="team",
        foreign_keys="GitAutomationState.teamId",
    )
    groupIssueHistory: Mapped[bool] = mapped_column(Boolean, nullable=False)
    icon: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    inheritIssueEstimation: Mapped[bool] = mapped_column(Boolean, nullable=False)
    inheritWorkflowStatuses: Mapped[bool] = mapped_column(Boolean, nullable=False)
    inheritProductIntelligenceScope: Mapped[Optional[bool]] = mapped_column(
        Boolean, nullable=True
    )
    productIntelligenceScope: Mapped[Optional[str]] = mapped_column(
        String, nullable=True
    )
    integrationsSettings: Mapped[Optional["IntegrationsSettings"]] = relationship(
        "IntegrationsSettings",
        back_populates="team",
        foreign_keys="IntegrationsSettings.teamId",
        uselist=False,
    )
    inviteHash: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    issueCount: Mapped[int] = mapped_column(Integer, nullable=False)
    issueEstimationAllowZero: Mapped[bool] = mapped_column(Boolean, nullable=False)
    issueEstimationExtended: Mapped[bool] = mapped_column(Boolean, nullable=False)
    issueEstimationType: Mapped[str] = mapped_column(String, nullable=False)
    issueOrderingNoPriorityFirst: Mapped[bool] = mapped_column(Boolean, nullable=False)
    issueSortOrderDefaultToBottom: Mapped[bool] = mapped_column(Boolean, nullable=False)
    joinByDefault: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    key: Mapped[str] = mapped_column(String, nullable=False)
    labels: Mapped[list["IssueLabel"]] = relationship(
        "IssueLabel", back_populates="team", foreign_keys="IssueLabel.teamId"
    )
    markedAsDuplicateWorkflowStateId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("workflow_states.id"), nullable=True
    )
    markedAsDuplicateWorkflowState: Mapped[Optional["WorkflowState"]] = relationship(
        "WorkflowState",
        back_populates="markedAsDuplicateForTeam",
        foreign_keys=[markedAsDuplicateWorkflowStateId],
        uselist=False,
    )
    # membership skipped
    memberships: Mapped[list["TeamMembership"]] = relationship(
        "TeamMembership",
        back_populates="team",
        foreign_keys="TeamMembership.teamId",
    )
    mergeWorkflowStateId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("workflow_states.id"), nullable=True
    )
    mergeWorkflowState: Mapped[Optional["WorkflowState"]] = relationship(
        "WorkflowState",
        back_populates="mergeForTeam",
        foreign_keys=[mergeWorkflowStateId],
        uselist=False,
    )
    mergeableWorkflowStateId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("workflow_states.id"), nullable=True
    )
    mergeableWorkflowState: Mapped[Optional["WorkflowState"]] = relationship(
        "WorkflowState",
        back_populates="mergeableForTeam",
        foreign_keys=[mergeableWorkflowStateId],
        uselist=False,
    )
    name: Mapped[str] = mapped_column(String, nullable=False)
    organizationId: Mapped[str] = mapped_column(
        ForeignKey("organizations.id"), nullable=False
    )
    organization: Mapped["Organization"] = relationship(
        "Organization", back_populates="teams", foreign_keys=[organizationId]
    )
    posts: Mapped[list["Post"]] = relationship(
        "Post", back_populates="team", foreign_keys="Post.teamId"
    )
    private: Mapped[bool] = mapped_column(Boolean, nullable=False)
    progressHistory: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    projects: Mapped[list["Project"]] = relationship(
        "Project",
        secondary=team_project_association,
        back_populates="teams",
    )
    requirePriorityToLeaveTriage: Mapped[bool] = mapped_column(Boolean, nullable=False)
    reviewWorkflowStateId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("workflow_states.id"), nullable=True
    )
    reviewWorkflowState: Mapped[Optional["WorkflowState"]] = relationship(
        "WorkflowState",
        back_populates="reviewForTeam",
        foreign_keys=[reviewWorkflowStateId],
        uselist=False,
    )
    scimGroupName: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    scimManaged: Mapped[bool] = mapped_column(Boolean, nullable=False)
    setIssueSortOrderOnStateChange: Mapped[str] = mapped_column(String, nullable=False)
    slackIssueComments: Mapped[bool] = mapped_column(Boolean, nullable=False)
    slackIssueStatuses: Mapped[bool] = mapped_column(Boolean, nullable=False)
    slackNewIssue: Mapped[bool] = mapped_column(Boolean, nullable=False)
    startWorkflowStateId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("workflow_states.id"), nullable=True
    )
    startWorkflowState: Mapped[Optional["WorkflowState"]] = relationship(
        "WorkflowState",
        back_populates="startForTeam",
        foreign_keys=[startWorkflowStateId],
        uselist=False,
    )
    templates: Mapped[list["Template"]] = relationship(
        "Template", back_populates="team", foreign_keys="Template.teamId"
    )
    timezone: Mapped[str] = mapped_column(String, nullable=False)
    triageEnabled: Mapped[bool] = mapped_column(Boolean, nullable=False)
    triageIssueStateId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("workflow_states.id"), nullable=True
    )
    triageIssueState: Mapped[Optional["WorkflowState"]] = relationship(
        "WorkflowState",
        back_populates="triageForTeam",
        foreign_keys=[triageIssueStateId],
        uselist=False,
    )
    triageResponsibility: Mapped[Optional["TriageResponsibility"]] = relationship(
        "TriageResponsibility",
        back_populates="team",
        foreign_keys="TriageResponsibility.teamId",
        uselist=False,
    )
    upcomingCycleCount: Mapped[float] = mapped_column(Float, nullable=False)
    updatedAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    webhooks: Mapped[list["Webhook"]] = relationship(
        "Webhook", back_populates="team", foreign_keys="Webhook.teamId"
    )


class TeamMembership(Base):
    __tablename__ = "team_memberships"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    userId: Mapped[str] = mapped_column(ForeignKey("users.id"), nullable=False)
    user: Mapped["User"] = relationship(
        "User", back_populates="teamMemberships", foreign_keys=[userId]
    )
    teamId: Mapped[str] = mapped_column(ForeignKey("teams.id"), nullable=False)
    team: Mapped["Team"] = relationship(
        "Team", back_populates="teamMemberships", foreign_keys=[teamId]
    )
    archivedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    createdAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    owner: Mapped[bool] = mapped_column(Boolean, nullable=False)
    sortOrder: Mapped[float] = mapped_column(Float, nullable=False)
    updatedAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)


class Template(Base):
    __tablename__ = "templates"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    teamId: Mapped[Optional[str]] = mapped_column(ForeignKey("teams.id"), nullable=True)
    team: Mapped[Optional["Team"]] = relationship(
        "Team", back_populates="templates", foreign_keys=[teamId]
    )
    defaultForTeam: Mapped[Optional["Team"]] = relationship(
        "Team",
        back_populates="defaultProjectTemplate",
        foreign_keys="Team.defaultProjectTemplateId",
        uselist=False,
    )
    defaultForMembersTeam: Mapped[Optional["Team"]] = relationship(
        "Team",
        back_populates="defaultTemplateForMembers",
        foreign_keys="Team.defaultTemplateForMembersId",
        uselist=False,
    )
    defaultForNonMembersTeam: Mapped[Optional["Team"]] = relationship(
        "Team",
        back_populates="defaultTemplateForNonMembers",
        foreign_keys="Team.defaultTemplateForNonMembersId",
        uselist=False,
    )
    organizationId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("organizations.id"), nullable=True
    )
    organization: Mapped[Optional["Organization"]] = relationship(
        "Organization", back_populates="templates", foreign_keys=[organizationId]
    )


class Organization(Base):
    __tablename__ = "organizations"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    users: Mapped[list["User"]] = relationship(
        "User", back_populates="organization", foreign_keys="User.organizationId"
    )
    teams: Mapped[list["Team"]] = relationship(
        "Team", back_populates="organization", foreign_keys="Team.organizationId"
    )
    aiAddonEnabled: Mapped[bool] = mapped_column(Boolean, nullable=False)
    aiTelemetryEnabled: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    allowMembersToInvite: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    allowedAuthServices: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    allowedFileUploadContentTypes: Mapped[list[str]] = mapped_column(
        JSON, nullable=False
    )
    archivedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    createdAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    createdIssueCount: Mapped[int] = mapped_column(Integer, nullable=False)
    customerCount: Mapped[int] = mapped_column(Integer, nullable=False)
    customersConfiguration: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    customersEnabled: Mapped[bool] = mapped_column(Boolean, nullable=False)
    defaultFeedSummarySchedule: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    deletionRequestedAt: Mapped[Optional[datetime]] = mapped_column(
        DateTime, nullable=True
    )
    facets: Mapped[list["Facet"]] = relationship(
        "Facet",
        back_populates="sourceOrganization",
        foreign_keys="Facet.sourceOrganizationId",
    )
    feedEnabled: Mapped[bool] = mapped_column(Boolean, nullable=False)
    fiscalYearStartMonth: Mapped[float] = mapped_column(Float, nullable=False)
    gitBranchFormat: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    gitLinkbackMessagesEnabled: Mapped[bool] = mapped_column(Boolean, nullable=False)
    gitPublicLinkbackMessagesEnabled: Mapped[bool] = mapped_column(
        Boolean, nullable=False
    )
    hipaaComplianceEnabled: Mapped[bool] = mapped_column(Boolean, nullable=False)
    initiativeUpdateReminderFrequencyInWeeks: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True
    )
    initiativeUpdateRemindersDay: Mapped[str] = mapped_column(String, nullable=False)
    initiativeUpdateRemindersHour: Mapped[float] = mapped_column(Float, nullable=False)
    integrations: Mapped[list["Integration"]] = relationship(
        "Integration",
        back_populates="organization",
        foreign_keys="Integration.organizationId",
    )
    # ipRestrictions skipped
    labels: Mapped[list["IssueLabel"]] = relationship(
        "IssueLabel",
        back_populates="organization",
        foreign_keys="IssueLabel.organizationId",
    )
    logoUrl: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    oauthAppReview: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    personalApiKeysEnabled: Mapped[Optional[bool]] = mapped_column(
        Boolean, nullable=True
    )
    periodUploadVolume: Mapped[float] = mapped_column(Float, nullable=False)
    previousUrlKeys: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    projectLabels: Mapped[list["ProjectLabel"]] = relationship(
        "ProjectLabel",
        back_populates="organization",
        foreign_keys="ProjectLabel.organizationId",
    )
    projectUpdateReminderFrequencyInWeeks: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True
    )
    projectUpdateRemindersDay: Mapped[str] = mapped_column(String, nullable=False)
    projectUpdateRemindersHour: Mapped[float] = mapped_column(Float, nullable=False)
    projectUpdatesReminderFrequency: Mapped[str] = mapped_column(String, nullable=False)
    projectStatuses: Mapped[list["ProjectStatus"]] = relationship(
        "ProjectStatus", foreign_keys="ProjectStatus.organizationId"
    )
    reducedPersonalInformation: Mapped[Optional[bool]] = mapped_column(
        Boolean, nullable=True
    )
    releaseChannel: Mapped[str] = mapped_column(String, nullable=False)
    restrictAgentInvocationToMembers: Mapped[Optional[bool]] = mapped_column(
        Boolean, nullable=True
    )
    restrictLabelManagementToAdmins: Mapped[Optional[bool]] = mapped_column(
        Boolean, nullable=True
    )
    restrictTeamCreationToAdmins: Mapped[Optional[bool]] = mapped_column(
        Boolean, nullable=True
    )
    roadmapEnabled: Mapped[bool] = mapped_column(Boolean, nullable=False)
    samlEnabled: Mapped[bool] = mapped_column(Boolean, nullable=False)
    samlSettings: Mapped[Optional[dict[str, Any]]] = mapped_column(JSON, nullable=True)
    scimEnabled: Mapped[bool] = mapped_column(Boolean, nullable=False)
    scimSettings: Mapped[Optional[dict[str, Any]]] = mapped_column(JSON, nullable=True)
    slaDayCount: Mapped[str] = mapped_column(String, nullable=False)
    slaEnabled: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    subscription: Mapped[Optional["PaidSubscription"]] = relationship(
        "PaidSubscription",
        back_populates="organization",
        foreign_keys="PaidSubscription.organizationId",
        uselist=False,
    )
    templates: Mapped[list["Template"]] = relationship(
        "Template",
        back_populates="organization",
        foreign_keys="Template.organizationId",
    )
    themeSettings: Mapped[Optional[dict[str, Any]]] = mapped_column(JSON, nullable=True)
    trialEndsAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    updatedAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    urlKey: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    userCount: Mapped[int] = mapped_column(Integer, nullable=False)
    workingDays: Mapped[list[float]] = mapped_column(JSON, nullable=False)


class User(Base):
    __tablename__ = "users"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    asksRequestedIssues: Mapped[list["Issue"]] = relationship(
        "Issue", back_populates="asksRequester", foreign_keys="Issue.asksRequesterId"
    )
    assignedIssues: Mapped[list["Issue"]] = relationship(
        "Issue", back_populates="assignee", foreign_keys="Issue.assigneeId"
    )
    createdIssues: Mapped[list["Issue"]] = relationship(
        "Issue", back_populates="creator", foreign_keys="Issue.creatorId"
    )
    delegatedIssues: Mapped[list["Issue"]] = relationship(
        "Issue", back_populates="delegate", foreign_keys="Issue.delegateId"
    )
    snoozedIssues: Mapped[list["Issue"]] = relationship(
        "Issue", back_populates="snoozedBy", foreign_keys="Issue.snoozedById"
    )
    subscribedIssues: Mapped[list["Issue"]] = relationship(
        "Issue",
        secondary=issue_subscriber_user_association,
        back_populates="subscribers",
    )
    active: Mapped[bool] = mapped_column(Boolean, nullable=False)
    admin: Mapped[bool] = mapped_column(Boolean, nullable=False)
    app: Mapped[bool] = mapped_column(Boolean, nullable=False)
    archivedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    avatarBackgroundColor: Mapped[str] = mapped_column(String, nullable=False)
    avatarUrl: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    calendarHash: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    canAccessAnyPublicTeam: Mapped[bool] = mapped_column(Boolean, nullable=False)
    createdAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    createdIssueCount: Mapped[int] = mapped_column(Integer, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    disableReason: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    displayName: Mapped[str] = mapped_column(String, nullable=False)
    drafts: Mapped[list["Draft"]] = relationship(
        "Draft", back_populates="user", foreign_keys="Draft.userId"
    )
    email: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    gitHubUserId: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    discordUserId: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    guest: Mapped[bool] = mapped_column(Boolean, nullable=False)
    # identityProvider skipped
    initials: Mapped[str] = mapped_column(String, nullable=False)
    inviteHash: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    isAssignable: Mapped[bool] = mapped_column(Boolean, nullable=False)
    isMe: Mapped[bool] = mapped_column(Boolean, nullable=False)
    isMentionable: Mapped[bool] = mapped_column(Boolean, nullable=False)
    issueDrafts: Mapped[list["IssueDraft"]] = relationship(
        "IssueDraft", back_populates="creator", foreign_keys="IssueDraft.creatorId"
    )
    lastSeen: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    organizationId: Mapped[str] = mapped_column(
        ForeignKey("organizations.id"), nullable=False
    )
    organization: Mapped["Organization"] = relationship(
        "Organization", back_populates="users", foreign_keys=[organizationId]
    )
    statusEmoji: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    statusLabel: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    statusUntilAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    teamMemberships: Mapped[list["TeamMembership"]] = relationship(
        "TeamMembership",
        back_populates="user",
        foreign_keys="TeamMembership.userId",
    )
    teams: Mapped[list["Team"]] = relationship(
        "Team",
        secondary="team_memberships",
        back_populates="members",
        viewonly=True,
    )
    timezone: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    updatedAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    url: Mapped[str] = mapped_column(String, nullable=False)
    settings: Mapped[Optional["UserSettings"]] = relationship(
        "UserSettings",
        back_populates="user",
        foreign_keys="UserSettings.userId",
        uselist=False,
    )


class UserFlag(Base):
    __tablename__ = "user_flags"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    userId: Mapped[str] = mapped_column(ForeignKey("users.id"), nullable=False)
    user: Mapped["User"] = relationship("User", foreign_keys=[userId])
    flag: Mapped[str] = mapped_column(String, nullable=False)
    value: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    lastSyncId: Mapped[float] = mapped_column(Float, nullable=False)
    createdAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    updatedAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)


class WorkflowState(Base):
    __tablename__ = "workflow_states"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    teamId: Mapped[str] = mapped_column(ForeignKey("teams.id"), nullable=False)
    team: Mapped["Team"] = relationship(
        "Team", back_populates="workflowStates", foreign_keys=[teamId]
    )
    defaultForTeam: Mapped[Optional["Team"]] = relationship(
        "Team",
        back_populates="defaultIssueState",
        foreign_keys="Team.defaultIssueStateId",
        uselist=False,
    )
    issues: Mapped[list["Issue"]] = relationship(
        "Issue", back_populates="state", foreign_keys="Issue.stateId"
    )
    draftForTeam: Mapped[Optional["Team"]] = relationship(
        "Team",
        back_populates="draftWorkflowState",
        foreign_keys="Team.draftWorkflowStateId",
        uselist=False,
    )
    markedAsDuplicateForTeam: Mapped[Optional["Team"]] = relationship(
        "Team",
        back_populates="markedAsDuplicateWorkflowState",
        foreign_keys="Team.markedAsDuplicateWorkflowStateId",
        uselist=False,
    )
    mergeForTeam: Mapped[Optional["Team"]] = relationship(
        "Team",
        back_populates="mergeWorkflowState",
        foreign_keys="Team.mergeWorkflowStateId",
        uselist=False,
    )
    mergeableForTeam: Mapped[Optional["Team"]] = relationship(
        "Team",
        back_populates="mergeableWorkflowState",
        foreign_keys="Team.mergeableWorkflowStateId",
        uselist=False,
    )
    reviewForTeam: Mapped[Optional["Team"]] = relationship(
        "Team",
        back_populates="reviewWorkflowState",
        foreign_keys="Team.reviewWorkflowStateId",
        uselist=False,
    )
    startForTeam: Mapped[Optional["Team"]] = relationship(
        "Team",
        back_populates="startWorkflowState",
        foreign_keys="Team.startWorkflowStateId",
        uselist=False,
    )
    triageForTeam: Mapped[Optional["Team"]] = relationship(
        "Team",
        back_populates="triageIssueState",
        foreign_keys="Team.triageIssueStateId",
        uselist=False,
    )
    archivedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    color: Mapped[str] = mapped_column(String, nullable=False)
    createdAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    inheritedFromId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("workflow_states.id"), nullable=True
    )
    inheritedFrom: Mapped[Optional["WorkflowState"]] = relationship(
        "WorkflowState",
        back_populates="children",
        foreign_keys=[inheritedFromId],
        remote_side="WorkflowState.id",
    )
    children: Mapped[list["WorkflowState"]] = relationship(
        "WorkflowState",
        back_populates="inheritedFrom",
        foreign_keys="WorkflowState.inheritedFromId",
    )
    name: Mapped[str] = mapped_column(String, nullable=False)
    position: Mapped[float] = mapped_column(Float, nullable=False)
    type: Mapped[str] = mapped_column(String, nullable=False)
    updatedAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)


class Draft(Base):
    __tablename__ = "drafts"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    userId: Mapped[str] = mapped_column(ForeignKey("users.id"), nullable=False)
    user: Mapped[User] = relationship(
        "User", back_populates="drafts", foreign_keys=[userId]
    )


class IssueDraft(Base):
    __tablename__ = "issue_drafts"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    creatorId: Mapped[str] = mapped_column(ForeignKey("users.id"), nullable=False)
    creator: Mapped[User] = relationship(
        "User", back_populates="issueDrafts", foreign_keys=[creatorId]
    )


class Facet(Base):
    __tablename__ = "facets"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    sourceTeamId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("teams.id"), nullable=True
    )
    sourceTeam: Mapped[Optional["Team"]] = relationship(
        "Team",
        back_populates="facets",
        foreign_keys=[sourceTeamId],
    )
    sourceOrganizationId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("organizations.id"), nullable=True
    )
    sourceOrganization: Mapped[Optional["Organization"]] = relationship(
        "Organization",
        back_populates="facets",
        foreign_keys=[sourceOrganizationId],
    )
    sourceProjectId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("projects.id"), nullable=True
    )
    sourceProject: Mapped[Optional["Project"]] = relationship(
        "Project",
        back_populates="facets",
        foreign_keys=[sourceProjectId],
    )
    sourceInitiativeId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("initiatives.id"), nullable=True
    )
    sourceInitiative: Mapped[Optional["Initiative"]] = relationship(
        "Initiative",
        back_populates="facets",
        foreign_keys=[sourceInitiativeId],
    )


class GitAutomationState(Base):
    __tablename__ = "git_automation_states"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    teamId: Mapped[str] = mapped_column(ForeignKey("teams.id"), nullable=False)
    team: Mapped["Team"] = relationship(
        "Team", back_populates="gitAutomationStates", foreign_keys=[teamId]
    )


class IntegrationsSettings(Base):
    __tablename__ = "integrations_settings"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    teamId: Mapped[Optional[str]] = mapped_column(ForeignKey("teams.id"), nullable=True)
    team: Mapped[Optional["Team"]] = relationship(
        "Team",
        back_populates="integrationsSettings",
        foreign_keys=[teamId],
        uselist=False,
    )
    projectId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("projects.id"), nullable=True
    )
    project: Mapped[Optional["Project"]] = relationship(
        "Project",
        back_populates="integrationsSettings",
        foreign_keys=[projectId],
        uselist=False,
    )
    initiativeId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("initiatives.id"), nullable=True
    )
    initiative: Mapped[Optional["Initiative"]] = relationship(
        "Initiative",
        back_populates="integrationsSettings",
        foreign_keys=[initiativeId],
        uselist=False,
    )


class Post(Base):
    __tablename__ = "posts"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    teamId: Mapped[Optional[str]] = mapped_column(ForeignKey("teams.id"), nullable=True)
    team: Mapped[Optional["Team"]] = relationship(
        "Team", back_populates="posts", foreign_keys="Post.teamId"
    )
    archivedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    audioSummary: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    body: Mapped[str] = mapped_column(String, nullable=False)
    bodyData: Mapped[str] = mapped_column(String, nullable=False)
    createdAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    creatorId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("users.id"), nullable=True
    )
    creator: Mapped[Optional["User"]] = relationship("User", foreign_keys=[creatorId])
    editedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    evalLogId: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    feedSummaryScheduleAtCreate: Mapped[Optional[str]] = mapped_column(
        String, nullable=True
    )
    reactionData: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    slugId: Mapped[str] = mapped_column(String, nullable=False)
    title: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    ttlUrl: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    type: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    updatedAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    userId: Mapped[Optional[str]] = mapped_column(ForeignKey("users.id"), nullable=True)
    user: Mapped[Optional["User"]] = relationship("User", foreign_keys=[userId])
    writtenSummaryData: Mapped[Optional[dict[str, Any]]] = mapped_column(
        JSON, nullable=True
    )


class TriageResponsibility(Base):
    __tablename__ = "triage_responsibilities"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    teamId: Mapped[str] = mapped_column(ForeignKey("teams.id"), nullable=False)
    team: Mapped["Team"] = relationship(
        "Team", back_populates="triageResponsibility", foreign_keys=[teamId]
    )


class Webhook(Base):
    __tablename__ = "webhooks"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    teamId: Mapped[Optional[str]] = mapped_column(ForeignKey("teams.id"), nullable=True)
    team: Mapped[Optional["Team"]] = relationship(
        "Team", back_populates="webhooks", foreign_keys=[teamId]
    )


class Integration(Base):
    __tablename__ = "integrations"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    organizationId: Mapped[str] = mapped_column(
        ForeignKey("organizations.id"), nullable=False
    )
    organization: Mapped["Organization"] = relationship(
        "Organization", back_populates="integrations", foreign_keys=[organizationId]
    )


class PaidSubscription(Base):
    __tablename__ = "paid_subscriptions"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    organizationId: Mapped[str] = mapped_column(
        ForeignKey("organizations.id"), nullable=False
    )
    organization: Mapped["Organization"] = relationship(
        "Organization",
        back_populates="subscription",
        foreign_keys=[organizationId],
        uselist=False,
    )


class ProjectLabel(Base):
    __tablename__ = "project_labels"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    projects: Mapped[list["Project"]] = relationship(
        "Project",
        secondary=project_label_project_association,
        back_populates="labels",
    )
    organizationId: Mapped[str] = mapped_column(
        ForeignKey("organizations.id"), nullable=False
    )
    organization: Mapped["Organization"] = relationship(
        "Organization", back_populates="projectLabels", foreign_keys=[organizationId]
    )
    archivedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    parentId: Mapped[Optional[str]] = mapped_column(
        String, ForeignKey("project_labels.id"), nullable=True
    )
    parent: Mapped[Optional["ProjectLabel"]] = relationship(
        "ProjectLabel",
        foreign_keys=[parentId],
        remote_side=[id],
        back_populates="children",
    )
    children: Mapped[list["ProjectLabel"]] = relationship(
        "ProjectLabel",
        foreign_keys=[parentId],
        back_populates="parent",
        cascade="all, delete-orphan",
        single_parent=True,
    )
    color: Mapped[str] = mapped_column(String, nullable=False)
    createdAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    creatorId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("users.id"), nullable=True
    )
    creator: Mapped[Optional["User"]] = relationship("User", foreign_keys=[creatorId])
    description: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    isGroup: Mapped[bool] = mapped_column(Boolean, nullable=False)
    lastAppliedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    retiredAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    updatedAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)


class Document(Base):
    __tablename__ = "documents"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    projectId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("projects.id"), nullable=True
    )
    project: Mapped[Optional["Project"]] = relationship(
        "Project", back_populates="documents", foreign_keys=[projectId]
    )
    archivedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    color: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    comments: Mapped[list["Comment"]] = relationship(
        "Comment", foreign_keys="Comment.documentId"
    )
    content: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    contentState: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    createdAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    creatorId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("users.id"), nullable=True
    )
    creator: Mapped[Optional["User"]] = relationship("User", foreign_keys=[creatorId])
    documentContentId: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    hiddenAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    icon: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    initiativeId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("initiatives.id"), nullable=True
    )
    initiative: Mapped[Optional["Initiative"]] = relationship(
        "Initiative", back_populates="documents", foreign_keys="Document.initiativeId"
    )
    lastAppliedTemplateId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("templates.id"), nullable=True
    )
    lastAppliedTemplate: Mapped[Optional["Template"]] = relationship(
        "Template", foreign_keys=[lastAppliedTemplateId]
    )
    resourceFolderId: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    slugId: Mapped[str] = mapped_column(String, nullable=False)
    sortOrder: Mapped[float] = mapped_column(Float, nullable=False)
    teamId: Mapped[Optional[str]] = mapped_column(ForeignKey("teams.id"), nullable=True)
    team: Mapped[Optional["Team"]] = relationship("Team", foreign_keys=[teamId])
    title: Mapped[str] = mapped_column(String, nullable=False)
    trashed: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    updatedAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    updatedById: Mapped[Optional[str]] = mapped_column(
        ForeignKey("users.id"), nullable=True
    )
    updatedBy: Mapped[Optional["User"]] = relationship(
        "User", foreign_keys=[updatedById]
    )
    url: Mapped[str] = mapped_column(String, nullable=False)
    subscribers: Mapped[list["User"]] = relationship(
        "User", secondary=document_subscribers_association
    )


class Initiative(Base):
    __tablename__ = "initiatives"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    projects: Mapped[list["Project"]] = relationship(
        "Project",
        secondary=initiative_project_association,
        back_populates="initiatives",
    )
    documents: Mapped[list["Document"]] = relationship(
        "Document", back_populates="initiative", foreign_keys="Document.initiativeId"
    )
    archivedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    color: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    completedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    content: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    createdAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    creatorId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("users.id"), nullable=True
    )
    creator: Mapped[Optional["User"]] = relationship("User", foreign_keys=[creatorId])
    documentContent: Mapped[Optional["DocumentContent"]] = relationship(
        "DocumentContent",
        back_populates="initiative",
        foreign_keys="DocumentContent.initiativeId",
        uselist=False,
    )
    facets: Mapped[list["Facet"]] = relationship(
        "Facet",
        back_populates="sourceInitiative",
        foreign_keys="Facet.sourceInitiativeId",
    )
    frequencyResolution: Mapped[str] = mapped_column(String, nullable=False)
    health: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    healthUpdatedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    history: Mapped[list["InitiativeHistory"]] = relationship(
        "InitiativeHistory",
        back_populates="initiative",
        foreign_keys="InitiativeHistory.initiativeId",
    )
    icon: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    integrationsSettings: Mapped[Optional["IntegrationsSettings"]] = relationship(
        "IntegrationsSettings",
        back_populates="initiative",
        foreign_keys="IntegrationsSettings.initiativeId",
        uselist=False,
    )
    lastUpdateId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("initiative_updates.id"), nullable=True
    )
    lastUpdate: Mapped[Optional["InitiativeUpdate"]] = relationship(
        "InitiativeUpdate",
        back_populates="lastUpdateForInitiative",
        foreign_keys=[lastUpdateId],
        uselist=False,
    )
    updates: Mapped[list["InitiativeUpdate"]] = relationship(
        "InitiativeUpdate",
        back_populates="initiative",
        foreign_keys="InitiativeUpdate.initiativeId",
    )
    links: Mapped[list["EntityExternalLink"]] = relationship(
        "EntityExternalLink",
        back_populates="initiative",
        foreign_keys="EntityExternalLink.initiativeId",
    )
    name: Mapped[str] = mapped_column(String, nullable=False)
    organizationId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("organizations.id"), nullable=True
    )
    organization: Mapped[Optional["Organization"]] = relationship(
        "Organization", foreign_keys=[organizationId]
    )
    ownerId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("users.id"), nullable=True
    )
    owner: Mapped[Optional["User"]] = relationship("User", foreign_keys=[ownerId])
    parentInitiativeId: Mapped[Optional[str]] = mapped_column(
        String, ForeignKey("initiatives.id"), nullable=True
    )
    parentInitiative: Mapped[Optional["Initiative"]] = relationship(
        "Initiative",
        foreign_keys=[parentInitiativeId],
        remote_side=[id],
        back_populates="subInitiatives",
    )
    subInitiatives: Mapped[list["Initiative"]] = relationship(
        "Initiative",
        foreign_keys=[parentInitiativeId],
        back_populates="parentInitiative",
        cascade="all, delete-orphan",
        single_parent=True,
    )
    slugId: Mapped[str] = mapped_column(String, nullable=False)
    sortOrder: Mapped[float] = mapped_column(Float, nullable=False)
    startedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    status: Mapped[str] = mapped_column(String, nullable=False)
    targetDate: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    targetDateResolution: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    trashed: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    updateReminderFrequency: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True
    )
    updateReminderFrequencyInWeeks: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True
    )
    updateRemindersDay: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    updateRemindersHour: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    updatedAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    url: Mapped[str] = mapped_column(String, nullable=False)


class ProjectHistory(Base):
    __tablename__ = "project_histories"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    projectId: Mapped[str] = mapped_column(ForeignKey("projects.id"), nullable=False)
    project: Mapped["Project"] = relationship(
        "Project", back_populates="history", foreign_keys=[projectId]
    )


class ProjectRelation(Base):
    __tablename__ = "project_relations"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    projectId: Mapped[str] = mapped_column(ForeignKey("projects.id"), nullable=False)
    project: Mapped["Project"] = relationship(
        "Project",
        back_populates="inverseRelations",
        foreign_keys=[projectId],
    )
    relatedProjectId: Mapped[str] = mapped_column(
        ForeignKey("projects.id"), nullable=False
    )
    relatedProject: Mapped["Project"] = relationship(
        "Project",
        back_populates="relations",
        foreign_keys=[relatedProjectId],
    )
    anchorType: Mapped[str] = mapped_column(String, nullable=False)
    archivedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    createdAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    relatedAnchorType: Mapped[str] = mapped_column(String, nullable=False)
    type: Mapped[str] = mapped_column(String, nullable=False)
    updatedAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    projectMilestoneId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("project_milestones.id"), nullable=True
    )
    projectMilestone: Mapped[Optional["ProjectMilestone"]] = relationship(
        "ProjectMilestone", foreign_keys=[projectMilestoneId]
    )
    relatedProjectMilestoneId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("project_milestones.id"), nullable=True
    )
    relatedProjectMilestone: Mapped[Optional["ProjectMilestone"]] = relationship(
        "ProjectMilestone", foreign_keys=[relatedProjectMilestoneId]
    )
    userId: Mapped[Optional[str]] = mapped_column(ForeignKey("users.id"), nullable=True)
    user: Mapped[Optional["User"]] = relationship("User", foreign_keys=[userId])


class EntityExternalLink(Base):
    __tablename__ = "entity_external_links"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    initiativeId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("initiatives.id"), nullable=True
    )
    initiative: Mapped[Optional["Initiative"]] = relationship(
        "Initiative", back_populates="links", foreign_keys=[initiativeId]
    )


class InitiativeHistory(Base):
    __tablename__ = "initiative_histories"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    initiativeId: Mapped[str] = mapped_column(
        ForeignKey("initiatives.id"), nullable=False
    )
    initiative: Mapped["Initiative"] = relationship(
        "Initiative", back_populates="history", foreign_keys=[initiativeId]
    )


organization_invite_team_association = Table(
    "organization_invite_team",
    Base.metadata,
    Column(
        "organization_invite_id",
        ForeignKey("organization_invites.id"),
        primary_key=True,
    ),
    Column("team_id", ForeignKey("teams.id"), primary_key=True),
)


class OrganizationInvite(Base):
    __tablename__ = "organization_invites"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    acceptedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    archivedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    createdAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    email: Mapped[str] = mapped_column(String, nullable=False)
    expiresAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    external: Mapped[bool] = mapped_column(Boolean, nullable=False)
    inviteeId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("users.id"), nullable=True
    )
    invitee: Mapped[Optional["User"]] = relationship("User", foreign_keys=[inviteeId])
    inviterId: Mapped[str] = mapped_column(ForeignKey("users.id"), nullable=False)
    inviter: Mapped["User"] = relationship("User", foreign_keys=[inviterId])
    metadata_: Mapped[Optional[dict[str, Any]]] = mapped_column(
        "metadata", JSON, nullable=True
    )
    organizationId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("organizations.id"), nullable=True
    )
    organization: Mapped[Optional["Organization"]] = relationship(
        "Organization", foreign_keys=[organizationId]
    )
    role: Mapped[str] = mapped_column(String, nullable=False)
    updatedAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    teams: Mapped[list["Team"]] = relationship(
        "Team", secondary=organization_invite_team_association
    )


class OrganizationDomain(Base):
    __tablename__ = "organization_domains"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    archivedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    authType: Mapped[str] = mapped_column(String, nullable=False)
    claimed: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    createdAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    creatorId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("users.id"), nullable=True
    )
    creator: Mapped[Optional["User"]] = relationship("User", foreign_keys=[creatorId])
    disableOrganizationCreation: Mapped[Optional[bool]] = mapped_column(
        Boolean, nullable=True
    )
    identityProviderId: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    updatedAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    verificationEmail: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    verified: Mapped[bool] = mapped_column(Boolean, nullable=False)


class Notification(Base):
    __tablename__ = "notifications"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    actorId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("users.id"), nullable=True
    )
    actor: Mapped[Optional["User"]] = relationship("User", foreign_keys=[actorId])
    actorAvatarColor: Mapped[str] = mapped_column(String, nullable=False)
    actorAvatarUrl: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    actorInitials: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    archivedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    # botActor skipped
    category: Mapped[str] = mapped_column(String, nullable=False)
    createdAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    emailedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    externalUserActorId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("external_users.id"), nullable=True
    )
    externalUserActor: Mapped[Optional["ExternalUser"]] = relationship(
        "ExternalUser", foreign_keys=[externalUserActorId]
    )
    groupingKey: Mapped[str] = mapped_column(String, nullable=False)
    groupingPriority: Mapped[float] = mapped_column(Float, nullable=False)
    inboxUrl: Mapped[str] = mapped_column(String, nullable=False)
    isLinearActor: Mapped[bool] = mapped_column(Boolean, nullable=False)
    issueStatusType: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    projectUpdateHealth: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    readAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    snoozedUntilAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    subtitle: Mapped[str] = mapped_column(String, nullable=False)
    title: Mapped[str] = mapped_column(String, nullable=False)
    type: Mapped[str] = mapped_column(String, nullable=False)
    unsnoozedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    updatedAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    url: Mapped[str] = mapped_column(String, nullable=False)
    userId: Mapped[str] = mapped_column(ForeignKey("users.id"), nullable=False)
    user: Mapped["User"] = relationship("User", foreign_keys=[userId])
    issueId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("issues.id"), nullable=True
    )
    issue: Mapped[Optional["Issue"]] = relationship("Issue", foreign_keys=[issueId])
    initiativeId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("initiatives.id"), nullable=True
    )
    initiative: Mapped[Optional["Initiative"]] = relationship(
        "Initiative", foreign_keys=[initiativeId]
    )
    initiativeUpdateId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("initiative_updates.id"), nullable=True
    )
    initiativeUpdate: Mapped[Optional["InitiativeUpdate"]] = relationship(
        "InitiativeUpdate", foreign_keys=[initiativeUpdateId]
    )
    projectId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("projects.id"), nullable=True
    )
    project: Mapped[Optional["Project"]] = relationship(
        "Project", foreign_keys=[projectId]
    )
    projectUpdateId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("project_updates.id"), nullable=True
    )
    projectUpdate: Mapped[Optional["ProjectUpdate"]] = relationship(
        "ProjectUpdate", foreign_keys=[projectUpdateId]
    )
    oauthClientApprovalId: Mapped[Optional[str]] = mapped_column(String, nullable=True)


class ExternalUser(Base):
    __tablename__ = "external_users"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    archivedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    avatarUrl: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    createdAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    displayName: Mapped[str] = mapped_column(String, nullable=False)
    email: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    lastSeen: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    organizationId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("organizations.id"), nullable=True
    )
    organization: Mapped[Optional["Organization"]] = relationship(
        "Organization", foreign_keys=[organizationId]
    )
    updatedAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)


class ProjectStatus(Base):
    __tablename__ = "project_statuses"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    organizationId: Mapped[str] = mapped_column(
        ForeignKey("organizations.id"), nullable=False
    )
    archivedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    color: Mapped[str] = mapped_column(String, nullable=False)
    createdAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    indefinite: Mapped[bool] = mapped_column(Boolean, nullable=False)
    name: Mapped[str] = mapped_column(String, nullable=False)
    position: Mapped[float] = mapped_column(Float, nullable=False)
    type: Mapped[str] = mapped_column(String, nullable=False)
    updatedAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)


class InitiativeRelation(Base):
    __tablename__ = "initiative_relations"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    archivedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    createdAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    sortOrder: Mapped[float] = mapped_column(Float, nullable=False)
    updatedAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    initiativeId: Mapped[str] = mapped_column(
        ForeignKey("initiatives.id"), nullable=False
    )
    initiative: Mapped["Initiative"] = relationship(
        "Initiative", foreign_keys=[initiativeId]
    )
    relatedInitiativeId: Mapped[str] = mapped_column(
        ForeignKey("initiatives.id"), nullable=False
    )
    relatedInitiative: Mapped["Initiative"] = relationship(
        "Initiative", foreign_keys=[relatedInitiativeId]
    )
    userId: Mapped[Optional[str]] = mapped_column(ForeignKey("users.id"), nullable=True)
    user: Mapped[Optional["User"]] = relationship("User", foreign_keys=[userId])


class InitiativeToProject(Base):
    __tablename__ = "initiative_to_projects"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    archivedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    createdAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    initiativeId: Mapped[str] = mapped_column(
        ForeignKey("initiatives.id"), nullable=False
    )
    initiative: Mapped["Initiative"] = relationship(
        "Initiative", foreign_keys=[initiativeId]
    )
    projectId: Mapped[str] = mapped_column(ForeignKey("projects.id"), nullable=False)
    project: Mapped["Project"] = relationship("Project", foreign_keys=[projectId])
    sortOrder: Mapped[str] = mapped_column(String, nullable=False)
    updatedAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)


class IssueImport(Base):
    __tablename__ = "issue_imports"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    archivedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    createdAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    creatorId: Mapped[Optional[str]] = mapped_column(
        ForeignKey("users.id"), nullable=True
    )
    creator: Mapped[Optional["User"]] = relationship("User", foreign_keys=[creatorId])
    csvFileUrl: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    displayName: Mapped[str] = mapped_column(String, nullable=False)
    error: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    errorMetadata: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    mapping: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    progress: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    service: Mapped[str] = mapped_column(String, nullable=False)
    serviceMetadata: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    status: Mapped[str] = mapped_column(String, nullable=False)
    teamName: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    updatedAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)


class UserSettings(Base):
    """The settings of a user as a JSON object."""

    __tablename__ = "user_settings"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    userId: Mapped[str] = mapped_column(
        ForeignKey("users.id"), nullable=False, unique=True
    )
    user: Mapped["User"] = relationship(
        "User", back_populates="settings", foreign_keys=[userId]
    )
    archivedAt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    autoAssignToSelf: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False
    )
    calendarHash: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    createdAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    # Notification preferences stored as JSON
    notificationCategoryPreferences: Mapped[dict] = mapped_column(
        JSON, nullable=False, default=dict
    )
    notificationChannelPreferences: Mapped[dict] = mapped_column(
        JSON, nullable=False, default=dict
    )
    notificationDeliveryPreferences: Mapped[dict] = mapped_column(
        JSON, nullable=False, default=dict
    )
    showFullUserNames: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False
    )
    subscribedToChangelog: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True
    )
    subscribedToDPA: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False
    )
    subscribedToInviteAccepted: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True
    )
    subscribedToPrivacyLegalUpdates: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True
    )
    subscribedToGeneralMarketingCommunications: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False
    )
    unsubscribedFrom: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    updatedAt: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    # Additional settings fields
    feedSummarySchedule: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    settings: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    usageWarningHistory: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    
