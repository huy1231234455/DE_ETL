from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.types import BooleanType

spark= SparkSession.builder \
    .appName("DE_ETL") \
    .master("local[*]") \
    .config("spark.executor.memory","4g") \
    .getOrCreate()

schemaType= StructType([
    StructField("id",StringType(), True),
    StructField("type", StringType(), True),
    StructField("actor",StructType([
        StructField("id", IntegerType(), True),
        StructField("login",StringType(), True),
        StructField("gravatar_id", StringType(), True),
        StructField("url", StringType(), True),
        StructField("avatar_url", StringType(), True)
    ]),True),
    StructField("repo",StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("url", StringType(), True),
    ]),True),
    StructField("payload", StructType([
        #ForkEvent
        StructField("forkee",StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("full_name", StringType(), True),
            StructField("owner", StructType([
                StructField("login", StringType(), True),
                StructField("id", IntegerType(), True),
                StructField("avatar_url", StringType(), True),
                StructField("gravatar_id", StringType(), True),
                StructField("url", StringType(), True),
                StructField("html_url", StringType(), True),
                StructField("followers_url", StringType(), True),
                StructField("following_url", StringType(), True),
                StructField("gists_url", StringType(), True),
                StructField("starred_url", StringType(), True),
                StructField("subscriptions_url", StringType(), True),
                StructField("organizations_url", StringType(), True),
                StructField("repos_url", StringType(), True),
                StructField("events_url", StringType(), True),
                StructField("received_events_url", StringType(), True),
                StructField("type", StringType(), True),
                StructField("site_admin", BooleanType(), True),

            ]),True ),
            StructField("private",BooleanType(),True),
            StructField("html_url", StringType(), True),
            StructField("description", StringType(), True),
            StructField("fork", BooleanType(), True),
            StructField("url", StringType(), True),
            StructField("forks_url", StringType(), True),
            StructField("keys_url", StringType(), True),
            StructField("collaborators_url", StringType(), True),
            StructField("teams_url", StringType(), True),
            StructField("hooks_url", StringType(), True),
            StructField("issue_events_url", StringType(), True),
            StructField("events_url", StringType(), True),
            StructField("assignees_url", StringType(), True),
            StructField("branches_url", StringType(), True),
            StructField("tags_url", StringType(), True),
            StructField("blobs_url", StringType(), True),
            StructField("git_tags_url", StringType(), True),
            StructField("git_refs_url", StringType(), True),
            StructField("statuses_url", StringType(), True),
            StructField("languages_url", StringType(), True),
            StructField("stargazers_url", StringType(), True),
            StructField("contributors_url", StringType(), True),
            StructField("subscribers_url", StringType(), True),
            StructField("subscription_url", StringType(), True),
            StructField("commits_url", StringType(), True),
            StructField("git_commits_url", StringType(), True),
            StructField("comments_url", StringType(), True),
            StructField("issue_comment_url", StringType(), True),
            StructField("contents_url", StringType(), True),
            StructField("compare_url", StringType(), True),
            StructField("merges_url", StringType(), True),
            StructField("archive_url", StringType(), True),
            StructField("downloads_url", StringType(), True),
            StructField("issues_url", StringType(), True),
            StructField("pulls_url", StringType(), True),
            StructField("milestones_url", StringType(), True),
            StructField("notifications_url", StringType(), True),
            StructField("labels_url", StringType(), True),
            StructField("releases_url", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True),
            StructField("pushed_at", TimestampType(), True),
            StructField("git_url", StringType(), True),
            StructField("ssh_url", StringType(), True),
            StructField("clone_url", StringType(), True),
            StructField("svn_url", StringType(), True),
            StructField("homepage", StringType(), True),
            StructField("size", IntegerType(), True),
            StructField("stargazers_count", IntegerType(), True),
            StructField("watchers_count", IntegerType(), True),
            StructField("language", StringType(), True),
            StructField("has_issues", BooleanType(), True),
            StructField("has_downloads", BooleanType(), True),
            StructField("has_wiki", BooleanType(), True),
            StructField("has_pages", BooleanType(), True),
            StructField("forks_count", IntegerType(), True),
            StructField("mirror_url", StringType(), True),
            StructField("open_issues_count", IntegerType(), True),
            StructField("forks", IntegerType(), True),
            StructField("open_issues", IntegerType(), True),
            StructField("watchers", IntegerType(), True),
            StructField("default_branch", StringType(), True),
            StructField("public", BooleanType(), True),

        ]),True),
        # #PushEvent
        # StructField("push_id", IntegerType(), True),
        # StructField("size", IntegerType(), True),
        # StructField("distinct_size", IntegerType(), True),
        # StructField("ref", StringType(), True),
        # StructField("head", StringType(), True),
        # StructField("before", StringType(), True),
        # StructField("commits", ArrayType(StructType([
        #     StructField("sha", StringType(), True),
        #     StructField("author", StructType([
        #         StructField("email", StringType(), True),
        #         StructField("name", StringType(),True)
        #     ]), True),
        #     StructField("message", StringType(), True),
        #     StructField("distinct", BooleanType(), True),
        #     StructField("url", StringType(), True),
        #
        # ])), True),
        # #GollumEvent
        # StructField("pages", ArrayType(StructType([
        #     StructField("page_name", StringType(), True),
        #     StructField("title", StringType(), True),
        #     StructField("summary", StringType(), True),
        #     StructField("action", StringType(), True),
        #     StructField("sha", StringType(), True),
        #     StructField("html_url", StringType(), True),
        #
        # ]
        # )), True),
        # #ReleaseEvent
        # StructField("action", StringType(), True),
        # StructField("release", StructType([
        #     StructField("url", StringType(), True),
        #     StructField("assets_url", StringType(), True),
        #     StructField("upload_url", StringType(), True),
        #     StructField("html_url", StringType(), True),
        #     StructField("tag_name", IntegerType(), True),
        #     StructField("target_commitish", StringType(), True),
        #     StructField("name", StringType(), True),
        #     StructField("draft", BooleanType(), True),
        #     StructField("author", StructType([
        #         StructField("login", StringType(), True),
        #         StructField("id", IntegerType(), True),
        #         StructField("avatar_url", StringType(), True),
        #         StructField("gravatar_id", StringType(), True),
        #         StructField("url", StringType(), True),
        #         StructField("html_url", StringType(), True),
        #         StructField("followers_url", StringType(), True),
        #         StructField("following_url", StringType(), True),
        #         StructField("gists_url", StringType(), True),
        #         StructField("starred_url", StringType(), True),
        #         StructField("subscriptions_url", StringType(), True),
        #         StructField("organizations_url", StringType(), True),
        #         StructField("repos_url", StringType(), True),
        #         StructField("events_url", StringType(), True),
        #         StructField("received_events_url", StringType(), True),
        #         StructField("type", StringType(), True),
        #         StructField("site_admin", StringType(), True),
        #
        #     ]), True),
        #     StructField("prerelease", BooleanType(), True),
        #     StructField("created_at", TimestampType(), True),
        #     StructField("published_at", TimestampType(), True),
        #     StructField("assets", ArrayType(StringType()), True),
        #     StructField("tarball_url", StringType(), True),
        #     StructField("zipball_url", StringType(), True),
        #     StructField("body", StringType(), True),
        #
        # ]), True),
        # #CommitCommentEvent

    ]), True),
    StructField("public", BooleanType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("org",StructType([
        StructField("id", IntegerType(), True),
        StructField("login", StringType(), True),
        StructField("gravatar_id", StringType(), True),
        StructField("url", StringType(), True),
        StructField("avatar_url", StringType(), True),

    ]),True)

]
)
jsonData =  spark.read.schema(schemaType).json("json_data_example.json")
# jsonData =  spark.read.json("json_data_example.json")
# jsonData.select("type").distinct().show(truncate=False)
# jsonData.show(truncate=False)
# jsonData.filter("type == 'ReleaseEvent'").show(truncate=False)

jsonData.show(truncate=False)
# jsonData.select("payload.forkee.created_at").show(truncate=False)
jsonData.printSchema()