import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.gluetypes import *
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


def _find_null_fields(ctx, schema, path, output, nullStringSet, nullIntegerSet, frame):
    if isinstance(schema, StructType):
        for field in schema:
            new_path = path + "." if path != "" else path
            output = _find_null_fields(
                ctx,
                field.dataType,
                new_path + field.name,
                output,
                nullStringSet,
                nullIntegerSet,
                frame,
            )
    elif isinstance(schema, ArrayType):
        if isinstance(schema.elementType, StructType):
            output = _find_null_fields(
                ctx,
                schema.elementType,
                path,
                output,
                nullStringSet,
                nullIntegerSet,
                frame,
            )
    elif isinstance(schema, NullType):
        output.append(path)
    else:
        x, distinct_set = frame.toDF(), set()
        for i in x.select(path).distinct().collect():
            distinct_ = i[path.split(".")[-1]]
            if isinstance(distinct_, list):
                distinct_set |= set(
                    [
                        item.strip() if isinstance(item, str) else item
                        for item in distinct_
                    ]
                )
            elif isinstance(distinct_, str):
                distinct_set.add(distinct_.strip())
            else:
                distinct_set.add(distinct_)
        if isinstance(schema, StringType):
            if distinct_set.issubset(nullStringSet):
                output.append(path)
        elif (
            isinstance(schema, IntegerType)
            or isinstance(schema, LongType)
            or isinstance(schema, DoubleType)
        ):
            if distinct_set.issubset(nullIntegerSet):
                output.append(path)
    return output


def drop_nulls(
    glueContext, frame, nullStringSet, nullIntegerSet, transformation_ctx
) -> DynamicFrame:
    nullColumns = _find_null_fields(
        frame.glue_ctx, frame.schema(), "", [], nullStringSet, nullIntegerSet, frame
    )
    return DropFields.apply(
        frame=frame, paths=nullColumns, transformation_ctx=transformation_ctx
    )


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 Source
S3Source_node1697137979779 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://wednesday-solutions-intern/data/insurance.csv"],
        "recurse": True,
    },
    transformation_ctx="S3Source_node1697137979779",
)

# Script generated for node Count/Rows
SqlQuery1 = """
select count(*) from S3;
"""
CountRows_node1697138151803 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1,
    mapping={"S3": S3Source_node1697137979779},
    transformation_ctx="CountRows_node1697138151803",
)

# Script generated for node Drop Null Fields
DropNullFields_node1697138442035 = drop_nulls(
    glueContext,
    frame=S3Source_node1697137979779,
    nullStringSet={"", "null"},
    nullIntegerSet={},
    transformation_ctx="DropNullFields_node1697138442035",
)

# Script generated for node Display/Table
SqlQuery2 = """
select * from S3_Input_Table;
"""
DisplayTable_node1697138075825 = sparkSqlQuery(
    glueContext,
    query=SqlQuery2,
    mapping={"S3_Input_Table": S3Source_node1697137979779},
    transformation_ctx="DisplayTable_node1697138075825",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1697138608647 = DynamicFrame.fromDF(
    DropNullFields_node1697138442035.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1697138608647",
)

# Script generated for node Evaluate Data Quality (Multiframe)
EvaluateDataQualityMultiframe_node1697138941996_ruleset = """
    # Example rules: Completeness "colA" between 0.4 and 0.8, ColumnCount > 10
    Rules = [IsComplete "sex",IsComplete "age",IsComplete "bmi",IsComplete "smoker",IsComplete "children",IsComplete"region",IsComplete "charges"
        ,RowCount > 1200
    ]
"""

EvaluateDataQualityMultiframe_node1697138941996 = EvaluateDataQuality().process_rows(
    frame=DropDuplicates_node1697138608647,
    ruleset=EvaluateDataQualityMultiframe_node1697138941996_ruleset,
    publishing_options={
        "dataQualityEvaluationContext": "EvaluateDataQualityMultiframe_node1697138941996",
        "enableDataQualityCloudWatchMetrics": True,
        "enableDataQualityResultsPublishing": True,
    },
    additional_options={"performanceTuning.caching": "CACHE_NOTHING"},
)

# Script generated for node rowLevelOutcomes
rowLevelOutcomes_node1697139313191 = SelectFromCollection.apply(
    dfc=EvaluateDataQualityMultiframe_node1697138941996,
    key="rowLevelOutcomes",
    transformation_ctx="rowLevelOutcomes_node1697139313191",
)

# Script generated for node ruleOutcomes
ruleOutcomes_node1697139316611 = SelectFromCollection.apply(
    dfc=EvaluateDataQualityMultiframe_node1697138941996,
    key="ruleOutcomes",
    transformation_ctx="ruleOutcomes_node1697139316611",
)

# Script generated for node Final_Data
SqlQuery0 = """
select age,sex,bmi,children,smoker,region,charges from Final;
"""
Final_Data_node1697139658781 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"Final": rowLevelOutcomes_node1697139313191},
    transformation_ctx="Final_Data_node1697139658781",
)

# Script generated for node S3 Logs Output
S3LogsOutput_node1697139903197 = glueContext.write_dynamic_frame.from_options(
    frame=ruleOutcomes_node1697139316611.repartition(1),
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://wednesday-solutions-intern/logs/",
        "partitionKeys": [],
    },
    transformation_ctx="S3LogsOutput_node1697139903197",
)

# Script generated for node S3_Output
S3_Output_node1697139584303 = glueContext.write_dynamic_frame.from_options(
    frame=Final_Data_node1697139658781.repartition(1),
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "s3://wednesday-solutions-intern/data/",
        "partitionKeys": [],
    },
    transformation_ctx="S3_Output_node1697139584303",
)

job.commit()
