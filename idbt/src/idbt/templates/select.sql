select 
% for column in select_columns:
    ${ column }
% endfor
from {{"${table1}" }}