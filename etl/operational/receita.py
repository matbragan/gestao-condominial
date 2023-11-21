from etl.operational.source import operational_writer
from etl.operational.generic_financial import generic_treatment

operational_writer('receita', generic_treatment)
