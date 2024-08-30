<?php

use label\DB;

class OrderingFactorVariables
{
    use \checkPearErrorTrait;

    /**
     * max of X in the variable [OF_top_X_main_category]
     */
    const MAX_TOP_X = 100;
    const OF_PRECISION = 3;
    const LOG_NAME = "OrderingFactor";
    const PRIMARY_IMG_TYPE = "primary";

    const SA_VARIABLES = ["bruttoIncome", "seenOnScreen", "purchases", "impressionsAll", "clicksAll", "CTRAll"];
    const TEASER_VARIABLES = ["clicks", "impressions", "CTR"];
    const MAX_FORMULA_ERRORS=5000;
    private int $formula_errors=0;

    /**
     * constants for [availability_factor] variable
     */
    const AVAILABILITY_VARIABLES = ["availability_factor"];
    const SAVED_AUCTIONS_AVAILABILITY_FIELDS = ['available', 'available_container_id', 'available_weeks', 'available_date_change_type', 'available_date'];
    // possible values for `available_date_change_type` feild
    const AVAILABLE_TYPE_CONTAINER = 'container';
    const AVAILABLE_TYPE_WEEK = 'weeks';
    const AVAILABLE_TYPE_MANUAL = 'manual';
    /**
     * cache array for function OF_top_X_main_category
     * @var array
     */
    private array $cache_OF_top_X_main_category = [];

    /**
     * use $calcedDataProvider->getDataByKey($key)
     * @var object
     */
    private $calcedDataProvider;

    /**
     * data from saved_autions table
     * @var array
     */
    public array $sa_data;
    private array $sa_ids_filtered;

    /**
     * availability values for each shop
     * @var array
     */
    private $ava_data;

    /**
     * @var array
     */
    private array $cats_OF;

    /**
     * @var array
     */
    private array $cats_SA;
    /**
     * @var array
     */
    public array $teasers_data;

    /**
     * @var array
     */
    public array $teasers_variables;

    /**
     * on/off standard log message, default = on
     * @var bool
     */
    private bool $isLogging = true;
    /**
     * echo log eval true/false
     * @var bool
     */
    private bool $logEval = false;
    function __construct(bool $isLogging = true)
    {
        $this->isLogging = $isLogging;
    }

    /**
     * set sa ids for call step function without full preloader
     * @param array $ids
     * @return void
     */
    public function setSAids(array $ids){
        $this->sa_ids_filtered = $ids;

        // init empty sa_data
        $this->sa_data = [];
        foreach ($ids as $id ) {
            $this->sa_data[$id] = [];
        }
    } // setSAids

    function setIsLogging(bool $isLogging = true) {
        $this->isLogging = $isLogging;
    }

    /**
     * $formula validator, trying to eval $formula replacing 1 instead of possible variables
     * and return true if ok or false if it is something wrong
     * @param $formula
     * @return bool
     */
    public static function validateTeaserFormula($formula)
    {
        // empty is possible
        if ( empty($formula) ) return true;

        // checking
        $possibleVariables = array_merge(static::SA_VARIABLES, static::TEASER_VARIABLES, static::AVAILABILITY_VARIABLES);
        $formulaForEval = $formula;
        if (preg_match_all("/\[(.*?)\]/i", $formula, $matches_var))
        {
            // iteration by variables
            foreach( array_unique( $matches_var[1] ) as $variable) {
                if ( !in_array($variable, $possibleVariables) ) return false;
                $formulaForEval = str_replace("[$variable]", 1, $formulaForEval);
            }
        }

        // try to evaluate
        try {

            eval("\$formula_value = " . $formulaForEval . ";");

        } catch (\Throwable $ex) {
            unset($ex);
            return false;
        }

        return true;
    } // formulaValidate

    /**
     * STEP functions
     */

    /**
     * use $this->sa_ids_filtered as array of id of SA
     * @param $stepNumber
     * @return false|void
     * @throws Exception
     */
    private function step_catsSA( $stepNumber = 1) {
        /********************************************************
         * STEP, get list of category for each SA
         ********************************************************/
        $dbr = DB::getInstanceReconnectible(DB::USAGE_READ);
        $sql = "
            SELECT scsc.cat_id k
        	    , GROUP_CONCAT( DISTINCT CONCAT(scsc.saved_id,
        	                                    ':',ifnull(sa.main_assigned_category,0),
        	                                    ':',shop.id) ) sa_ids
	        FROM shop_catalogue_saved_cache scsc
	        JOIN saved_auctions as sa ON sa.id = scsc.saved_id 
	        JOIN shop ON sa.username = shop.username AND sa.siteid = shop.siteid   
	        WHERE scsc.saved_id in (".implode(",", $this->sa_ids_filtered).") 
	        GROUP BY scsc.cat_id
	    ";
//        echo $sql.PHP_EOL; // @debug
        $this->cats_SA = $dbr->getAssoc($sql);
        if (!$this->checkPearError($this->cats_SA)) return false;

        // explode SA list
        $this->cats_SA = array_map( fn($val) =>
        array_map( fn($vval) =>

            // ... it can add what needed to evaluate in formula
        array_combine(["sa_id","main_cat_id","shop_id"], explode(":", $vval))
            , explode(",", $val)
        )
            , $this->cats_SA);

        // fill "categories" array fo each SA of the $this->sa_data property
        foreach($this->cats_SA as $cat_id => $sa_arr) {
            foreach ($sa_arr as $sa_item) {
                $this->sa_data[ $sa_item["sa_id"] ]["categories"][] = $cat_id;
            }
        }

        if ($this->isLogging) echo "Step {$stepNumber}. Cat's with assigned SA's were preloaded, count of categories: ".count($this->cats_SA).PHP_EOL;

    } // step_cats_SA

    /**
     * fill $this->teasers_data
     * @param $stepNumber
     * @return false|void
     * @throws Exception
     */
    public function step_allTeasers( $stepNumber = 1 )
    {
        /********************************************************
         * STEP, get list of teasers for each SA
         ********************************************************/
        // primary teaser
        $dbr = DB::getInstanceReconnectible(DB::USAGE_READ);
        $sql = "
            SELECT sa.id sa_id, teaser.doc_id doc_id
                FROM saved_auctions sa
                JOIN (
                    SELECT saved_id as sa_id,
                           SUBSTRING_INDEX(GROUP_CONCAT(doc_id ORDER BY IF(img_type='primary',1,0) desc, ordering desc, doc_id),',',1) as doc_id
                    FROM saved_pic
                    WHERE inactive = 0
                    GROUP by saved_id
                ) teaser ON teaser.sa_id = IFNULL(sa.master_sa, sa.id)
            WHERE sa.id in (".implode(",", $this->sa_ids_filtered).")
        ";
//        echo "<pre>".$sql.PHP_EOL."</pre>>"; // @debug
        $primary_teasers = $dbr->getAll($sql);
        if (!$this->checkPearError($primary_teasers)) return false;

        // alt teasers
        $sql = "
            SELECT sa.id as sa_id 
                    , doc_id
                    , saved_pic.alt_teaser_color
                    , saved_pic.alt_teaser_whitesh
                    , saved_pic.alt_teaser_whitenosh
            FROM saved_pic 
            JOIN saved_auctions sa ON sa.master_sa = saved_pic.saved_id OR sa.id = saved_pic.saved_id
            WHERE (alt_teaser_color>0 OR alt_teaser_whitesh>0 OR alt_teaser_whitenosh>0)
                  AND sa.id in (".implode(",", $this->sa_ids_filtered).") 
        ";
//        echo $sql.PHP_EOL; // @debug
        $alt_teasers = $dbr->getAll($sql);
        if (!$this->checkPearError($alt_teasers)) return false;

        // prepare index array for found teasers by sa_id
        // **********************************************
        $this->teasers_data = [];
        $count_teasers = 0;

        // primary teaser
        foreach ($primary_teasers as $primary_teaser) {
            $this->teasers_data[ $primary_teaser->sa_id ][] =  ["doc_id" => $primary_teaser->doc_id, "doc_type" => "primary"];
            $count_teasers++;
        }

        // alt teasers
        foreach ($alt_teasers as $item) {
            foreach(["alt_teaser_color", "alt_teaser_whitesh", "alt_teaser_whitenosh"] as $alt_teaser_type) {
                if ($item->{$alt_teaser_type} == 1) {
                    $doc_type = str_replace("alt_teaser_", "", $alt_teaser_type);
                    if ($doc_type == "") {
                        if ($this->isLogging) echo "WARNING: wrong data structure on sa_id=" . $item->sa_id . PHP_EOL;
                        continue;
                    }
                    $this->teasers_data[$item->sa_id][] = ["doc_id" => $item->doc_id, "doc_type" => $doc_type];
                    $count_teasers++;
                } // if
            } // foreach
        } // foreach

        if ($this->isLogging) echo "Step {$stepNumber}. Teaser's data has uploaded for found SA's, count of teasers: ".$count_teasers.PHP_EOL;
    }

    /**
     * fill $this->teasers_variables
     * fill $this->sa_data[ $saved_id][ "CTRAll" | "clickALL" | "impressionALL" ]
     * @param $stepNumber
     * @return false|void
     * @throws Exception
     */
    public function step_CTR ( $stepNumber = 1 ) {
        $dbr = DB::getInstanceReconnectible(DB::USAGE_READ);
        $sql = "
            SELECT spcc.saved_id, spcc.doc_id, spcc.doc_type
                  , sum(spcc.impression) impressions, sum(spcc.click) clicks, sum(spcc.click)/sum(spcc.impression) CTR
	        FROM saved_pic_ctr_collect spcc
            JOIN saved_auctions sa ON sa.id = spcc.saved_id
            JOIN shop ON shop.username = sa.username AND shop.siteid = sa.siteid
	        WHERE saved_id in (".implode(",", $this->sa_ids_filtered).")
	              AND spcc.date > DATE_ADD(CURRENT_TIMESTAMP, INTERVAL -shop.ctr_period DAY)

	              -- JJ condition, https://trello.com/c/VRAYwbZd/21050-grzegorz-cie%C5%9Blik-ordering-factor-null-of-values
	              AND spcc.date NOT BETWEEN DATE('2024-04-04') AND DATE('2024-04-15')
	              
	        GROUP BY saved_id, doc_id, doc_type
        ";
//        echo $sql.PHP_EOL; // @debug

        $clickData = $dbr->getAll($sql);
        if (!$this->checkPearError( $clickData )) return false;
        $this->teasers_variables = [];

        // fill $this->teaserVariables and add values in the $sa_data
        foreach($clickData as $item) {
            $this->teasers_variables[$item->saved_id][$item->doc_id][$item->doc_type] =
                ["clicks" => $item->clicks, "impressions" => $item->impressions,  "CTR" => $item->CTR];
        }

        // *********************************************
        // calcultae [clicksAll] and [impressionsAll] for SA using only teaser's clicks and impressions
        // for this calculation using all teasers from $this->teasers_data
        foreach($this->teasers_data as $sa_id => $teasers)
        {
            // just in case
            if (!array_key_exists($sa_id, $this->sa_data)) continue;

            // teasers iteration
            foreach ($teasers as $teaser) {
                ["doc_id" => $doc_id, "doc_type" => $doc_type] = $teaser;
                foreach(["clicks", "impressions"] as $dataType) {
                    if ($doc_type == "primary") {
                        $this->sa_data[$sa_id][$dataType."All"] += ($this->teasers_variables[$sa_id][$doc_id]["color"][$dataType] ?: 0)
                            + ($this->teasers_variables[$sa_id][$doc_id]["whitesh"][$dataType] ?: 0);
                    } else {
                        $this->sa_data[$sa_id][$dataType."All"] += $this->teasers_variables[$sa_id][$doc_id][$doc_type][$dataType] ?: 0;
                    }
                } // foreach $dataType
            } // foreach $teaser

        } // foreach $teasers

        // calc CTRAll
        foreach ($this->sa_data as $saved_id => $item) {
            if ($item["impressionsAll"]>0) {
                $this->sa_data[$saved_id]["CTRAll"] = round($item["clicksAll"] / $item["impressionsAll"], 4);
            } else {
                $this->sa_data[$saved_id]["CTRAll"] = 0;
            }
        }
        if ($this->isLogging) echo "Step {$stepNumber}. Preloaded data for teaser's variables: [impressions], [clicks], [CTR] and calc sum of this for each SA, count: ".count($clickData).PHP_EOL;
//        echo "teaser_variable: "; print_r($this->teasers_variables); // @debug
    }


    /**
     * fill $this->containers_available_date for [alvailability_factor] variable
     * @param $stepNumber
     * @return void
     * @throws Exception
     */
    private function step_AvailabilityFactor( $stepNumber = 1 )
    {
        $dbr = DB::getInstanceReconnectible(DB::USAGE_READ);

        // preload containers available date
        $all_container_ids = [];
        foreach ($this->sa_data as $item) {
            if (
                isset($item["available_date_change_type"])
                && $item['available_date_change_type'] == static::AVAILABLE_TYPE_CONTAINER
                && isset($item["available_container_id"])
            )
            {
                $all_container_ids[] = $item["available_container_id"];
            }
        }
        if (empty($all_container_ids)) {
            $this->containers_available_date = [];
        } else {
            $this->containers_available_date = \Offer::getAvailabilityDateByContainersList([], $all_container_ids);
        }

        // preload availability values
        $sql = "
            SELECT shop.id shop_id
                   , shop.availability_factor on_stock_availability_factor
                   , saf.id shop_availability_factor_id
                   , saf.days_from
                   , saf.days_to
                   , saf.availability_factor
            FROM shop
            LEFT JOIN shop_availability_factor saf ON saf.shop_id = shop.id
            ORDER BY shop.id, saf.days_from
        ";
        $ava_data = $dbr->getAll($sql);
        if (!$this->checkPearError( $ava_data )) return false;

        // fill $this->ava_data
        $this->ava_data = [];
        foreach ($ava_data as $ava_data_item)
        {
            $shop_id = $ava_data_item->shop_id;

            // set on stock availability factor
            if (!isset($this->ava_data[$shop_id])) $this->ava_data[$shop_id] = ["out_of_stock"=>[]];
            $this->ava_data[$shop_id]["on_stock_availability_factor"] = $ava_data_item->on_stock_availability_factor;

            // set out of stock availability factor for days periods
            if (isset( $ava_data_item->shop_availability_factor_id )) {
                $this->ava_data[$shop_id]["out_of_stock"][] = [
                    "days_from" => $ava_data_item->days_from
                    , "days_to" => $ava_data_item->days_to
                    , "availability_factor" => $ava_data_item->availability_factor
                ];
            }
        }
//        echo print_r($this->ava_data); // @debug
        if ($this->isLogging) echo "Step {$stepNumber}. Container's availability dates have uploaded for containers: ".count($all_container_ids).PHP_EOL;
    } // step_AvailableFactor

    /**
     * preload data for variables in eval:
     * [bruttoIncome], [purchases], [impressions], [impressionsAll], [clicks], [clicksAll], [CTR], [CTRAll]
     * @param array $ids
     * @return false|void
     * @throws Exception
     */
    public function preloadDataTeaser(array $ids = []) {
        $dbr = DB::getInstanceReconnectible(DB::USAGE_READ);
        if ($this->isLogging) {
            echo "===".PHP_EOL;
            echo get_class($this).". Preloading all data needed for TEASER Ordering Factor evaluate".PHP_EOL;
            echo getConnectionLog($dbr->getDb());
        }
        /********************************************************
         * STEP 1, get list of category for each SA
         ********************************************************/

//        $ids = [8036]; // @debug
        $this->logEval = !empty($ids);

        $auctionsDataQuery_timed_brutto_income =  \OrderingFactorHelper::buildAuctionsDataQuery($ids, "timed_brutto_income");
        $auctionsDataQuery_timed_purchases =  \OrderingFactorHelper::buildAuctionsDataQuery($ids, "timed_purchases");

        $sql = "
            SELECT sa.id k
                 , ".implode(",", array_map(fn($v) => "sa.".$v.PHP_EOL, static::SAVED_AUCTIONS_AVAILABILITY_FIELDS))."
                 , shop.conditions_for_of_per_teaser
                 , shop.id shop_id
                 , ifnull(auc_timed_brutto_income.timed_brutto_income, 0) bruttoIncome
                 , ifnull(auc_timed_purchases.timed_purchases, 0) purchases
            FROM saved_auctions sa
            LEFT JOIN ({$auctionsDataQuery_timed_brutto_income}) auc_timed_brutto_income ON auc_timed_brutto_income.saved_id = sa.id    
            LEFT JOIN ({$auctionsDataQuery_timed_purchases}) auc_timed_purchases ON auc_timed_purchases.saved_id = sa.id
            JOIN shop ON shop.username = sa.username AND shop.siteid = sa.siteid
            WHERE ". (empty($ids) ? " sa.inactive=0 ": " 1 ")
                   .($ids?" AND sa.id in (".implode(",",$ids).")" :"")."
        ";
//		echo $sql.PHP_EOL; // @debug

        $st = time();
        $this->sa_data = $dbr->getAssoc($sql);
        $ft = time();
        if (!$this->checkPearError( $this->sa_data )) return false;
        if ( count($this->sa_data) == 0) {
            if ($this->isLogging) echo "ERROR: Not found any SA, stopped".PHP_EOL;
            return false;
        }
        $this->sa_ids_filtered = array_keys( $this->sa_data );
        if ($this->isLogging) echo "Step 1. Preloaded SA list with variables: [bruttoIncome], count of SA: ".count($this->sa_data)
                                    .", run time query=".($ft-$st)."sec.".PHP_EOL;
//        print_r($this->sa_data); // @debug

        /********************************************************
         * STEP 2, load all SA for calculate
         ********************************************************/
        $this->step_catsSA(2);

        /********************************************************
         * STEP 3, load all teasers for SA
         ********************************************************/
        $this->step_allTeasers(3);

        /********************************************************
         * STEP 4, load variables: [click] [impressions] [clickSA] [impressionsSA]
         ********************************************************/
        $this->step_CTR(4);

        /********************************************************
         * STEP 5, load variable: [available_factor]
         ********************************************************/

        $this->step_AvailabilityFactor(5);

    } // preloadDataTeaserOF

    /**
     * generate an OF for cat_id, sa_id, doc_id and doc_type
     * @param string $cheatFormula
     * @param object $calcedDataProvider use $calcedDataProvider->getDataByKey($key)
     * @return \Generator
     */
    public function evalFormulaTeaser( $cheatFormula = null, $calcedDataProvider = null ) : \Generator
    {
        $this->calcedDataProvider = $calcedDataProvider;
        //iteration by teasers
        foreach($this->teasers_data as $sa_id => $teasers)
        {
            foreach($teasers as $teaser)
            {
                ["doc_id"=>$doc_id, "doc_type"=>$doc_type] = $teaser;

                // stop if there are too many errors
                if ($this->formula_errors > static::MAX_FORMULA_ERRORS) {
                    if ($this->isLogging)  "Too many formula errors. Stopped.".PHP_EOL;
                    return;
                }

                // use $cheatFormula if it is
                if ($cheatFormula) {
                    $formula = $formulaWithVars  = $cheatFormula;
                } else {
                    $formula = $formulaWithVars = $this->sa_data[ $sa_id ]["conditions_for_of_per_teaser"];
                }
                $formula = str_replace(" ", "", $formula);
                $formula = str_replace(PHP_EOL, "", $formula);

                // OF should set to zero if $formula is empty
                if (!isset( $formula ) || $formula == "" ) {
                    $formula = "0";
                }

                // find and replace all variables in the formula
                // *************************************************
                if (preg_match_all("/\[(.*?)\]/i", $formula, $matches_var))
                {
                    // iteration by variables
                    foreach( array_unique( $matches_var[1] ) as $variable)
                    {
                        $value = 0;
                        if ( in_array($variable, static::SA_VARIABLES) ) {
                            if (!array_key_exists($sa_id, $this->sa_data)) {
                                $this->formula_errors++;
                                if ($this->isLogging) echo "WARNING: not found sa_id={$sa_id} in the preloaded data" . PHP_EOL;
                                continue;
                            }
                            if (array_key_exists($variable, $this->sa_data[$sa_id])) {
                                $value = $this->sa_data[$sa_id][$variable];
                            } else {
                                $this->formula_errors++;
                                if ($this->isLogging) echo "ERROR: not found variable {$variable} for sa_id={$sa_id} in the preloaded data (sa_data)" . PHP_EOL;
                            }

                        } elseif ( in_array($variable, static::TEASER_VARIABLES) ) {
                            if (array_key_exists($sa_id, $this->teasers_variables)
                                    && array_key_exists($doc_id, $this->teasers_variables[$sa_id])
                                    && ($doc_type == "primary"
                                        || array_key_exists($doc_type, $this->teasers_variables[$sa_id][$doc_id]))
                                )
                            {

                                // ****************************************
                                // [clicks], [impressions] and [CTR] in the formula

                                // for primary sum of "color" and "whitesh"
                                if ($doc_type == "primary") {
                                    $value = ($this->teasers_variables[$sa_id][$doc_id]["color"][$variable] ?: 0)
                                              + ($this->teasers_variables[$sa_id][$doc_id]["whitesh"][$variable] ?: 0);
                                } else {
                                    $value = $this->teasers_variables[$sa_id][$doc_id][$doc_type][$variable];
                                }

                            } else {
                                if($this->logEval) echo "WARNING: for variable [{$variable}] not found value for sa_id={$sa_id}, doc_id={$doc_id}, doc_type={$doc_type} in the preloaded data (teaser_variables)" . PHP_EOL;
                            }
                        } elseif ( in_array($variable, static::AVAILABILITY_VARIABLES) ) {

                            $value = $this->availability_factor($sa_id);

                        } elseif ( false ) {

                            // @todo it is a place to add another variables

                        } else {
                            $this->formula_errors++;
                            if ($this->isLogging) echo "ERROR: can not calculate variable=[{$variable}]".PHP_EOL;
                        }

                        $formula = str_replace("[{$variable}]", $value, $formula);

                    } // foreach  $variable
                } // if set variables

                // eval formula

                try {

                    eval("\$formula_value = " . $formula . ";");

                } catch (\Throwable $ex) {
                    $this->formula_errors++;
                    if ($this->isLogging) echo "ERROR: can not evaluate={$formulaWithVars} ".PHP_EOL;
                    $formula_value = 0;
                    unset($ex);
                }

                // log eval
                if($this->logEval) echo "EVAL for sa_id={$sa_id}, doc_id={$doc_id}, doc_type={$doc_type} formula='{$formulaWithVars}': ".$formula." result=".$formula_value.PHP_EOL; // @debug

                // check on infinite
                if (is_infinite($formula_value)) {
                    if($this->logEval) echo "WARNING: Division by zero (value was set to 0): {$formula} for sa_id={$sa_id}, doc_id={$doc_id}, doc_type={$doc_type}".PHP_EOL;
                    $formula_value=0;
                }

                // check on NaN
                if (is_nan($formula_value) || !is_numeric($formula_value)) {
                    if($this->logEval) echo "WARNING: NAN (Non Numeric value): {$formula} for sa_id={$sa_id}, doc_id={$doc_id}, doc_type={$doc_type}".PHP_EOL;
                    $formula_value=0;
                }

                // it is a problem if there aren't categories for sa
                if (empty($this->sa_data[$sa_id]["categories"])) {
                    if($this->logEval) echo "WARNING: not found any categories for sa_id={$sa_id} in the preloaded data".PHP_EOL;
                    continue;
                }

                // generate the same OF value for each assigned category
                foreach($this->sa_data[$sa_id]["categories"] as $cat_id) {

                    yield [
                        "sa_id" => $sa_id,
                        "cat_id" => $cat_id,
                        "doc_id" => $doc_id,
                        "doc_type" => $doc_type,
                        "ordering_factor" => round((float)$formula_value, static::OF_PRECISION)
                    ];

                }

            } // $teasers
        } // $this->teasers_data

    }

    /**
     * fill local variables:  $sa_data,  $cats_OF,  $cats_SA, $sa_ids_filtered
     *
     * @param array $ids
     * @return false|void
     * @throws Exception
     */
    public function preloadDataNewArticles(array $ids = []) {

        $dbr = DB::getInstanceReconnectible(DB::USAGE_READ);
        $this->logEval = !empty($ids);

        if ($this->isLogging) {
            echo "===".PHP_EOL;
            echo get_class($this).". Preloading all data needed for formula evaluate".PHP_EOL;
        }

        /********************************************************
        * STEP 1, load all SA activated no later than `shop.of_max_days_since_activation` days from now
        ********************************************************/
        $sql = "
            SELECT sa.id k, shop.of_formula, shop.of_max_days_since_activation, MAX(tl.updated) max_updated
            FROM saved_auctions sa
            JOIN shop ON shop.username = sa.username
                AND shop.siteid = sa.siteid
            LEFT JOIN total_log tl ON tl.TableID=sa.id
                AND " . get_table_name('saved_auctions', 'tl') . "
                AND " . get_field_name('inactive', 'tl') . "		
            WHERE  sa.inactive=0
                   AND (tl.New_value=0 OR tl.ID IS NULL)
                   ".($ids?" AND sa.id in (".implode(",",$ids).")" :"")."
            GROUP BY sa.id, shop.of_formula, shop.of_max_days_since_activation 
            HAVING  max_updated IS NULL
                OR max_updated >= (NOW() - INTERVAL IFNULL(of_max_days_since_activation, 30) DAY)
        ";
//		echo $sql.PHP_EOL; // @debug
        $this->sa_data = $dbr->getAssoc($sql);
        //$sa_ids = [340186];
        if (!$this->checkPearError( $this->sa_data )) return false;
        if ( count($this->sa_data) == 0) {
            if ($this->isLogging) echo "ERROR: Not found any new SA, stopped".PHP_EOL;
            return false;
        }
        $sa_ids = array_keys( $this->sa_data );
        if ($this->isLogging) echo "Step 1. New SA count by 1th condition, by 'shop.of_max_days_since_activation': ".count($sa_ids).PHP_EOL;

//        print_r( array_slice($this->sa_data, 0, 2, true) ); // @debug

        /********************************************************
        * STEP 2, find list of 'new' category, only one is usual (1685 id)
        *********************************************************/

        /*
         *  https://trello.com/c/Muxosa0D/19127-danuta-dolega-new-articles-of-slow-value-capture
         *  removed condition for new SA from query bellow:
         *  "AND use_for_new_articles = 1"
         */

        $sql = "SELECT id
                FROM prologis2.shop_catalogue
                WHERE category_type = 'new' 
                      AND date_limited = 1";
        $new_cat_ids = $dbr->getCol($sql);
        if (!$this->checkPearError( $new_cat_ids )) return false;
        if ( count( $new_cat_ids ) == 0 ) {
            if ($this->isLogging) "ERROR: NOT Found 'New' catalogues, stopped.".PHP_EOL;
            return false;
        }
        if ($this->isLogging) echo "Step 2. New catalogues list: ".implode(",", $new_cat_ids).PHP_EOL;

        /********************************************************
        * STEP 3, filter SA by category and `shop.of_days_for_new`
        ********************************************************/
        $sql = ' 
        SELECT sp_last.saved_id
		       /*, sp_last.shop_id
               , sp_last.cat_id
               , sp.par_value*/
        FROM (
            SELECT max(id) id, saved_id, shop_id, cat_id
            FROM (
                SELECT saved_params.*
                    , REPLACE( SUBSTRING_INDEX(par_key, "][", 1), "cal_cat[", "") shop_id
                    , REPLACE( SUBSTR(par_key, LOCATE("][", par_key)+2), "]", "") cat_id
                FROM saved_params
                WHERE par_key like "cal_cat[%][%]" 
            ) saved_params
            WHERE cat_id in ('.implode(",", $new_cat_ids).')  
                  AND saved_id in ('.implode(",", $sa_ids).')
            GROUP BY saved_id, shop_id, cat_id
        ) sp_last
        JOIN saved_params sp on sp_last.id=sp.id
        JOIN shop on shop.id = sp_last.shop_id
        WHERE sp.par_value IS NOT NULL
              AND DATE(sp.par_value) >= (NOW() - INTERVAL IFNULL(of_days_for_new, 30) DAY)
        ';
//        echo $sql.PHP_EOL; // @debug
        $this->sa_ids_filtered = $dbr->getCol($sql);
        if (!$this->checkPearError($this->sa_ids_filtered)) return false;
        if ( count( $this->sa_ids_filtered ) == 0 ) {
            if ($this->isLogging) echo "WARNING: Not found new SA by 2th condition, by 'shop.of_days_for_new.".PHP_EOL;
            return false;
        }
        if ($this->isLogging) echo "Step 3. New SA count by 2th condition, by 'shop.of_days_for_new': ".count($this->sa_ids_filtered).PHP_EOL;

        /********************************************************
        * STEP 4, get list of category for each SA
        ********************************************************/
        $sql = "
            SELECT scsc.cat_id k
        	    , GROUP_CONCAT( DISTINCT CONCAT(scsc.saved_id,
        	                                    ':',ifnull(sa.main_assigned_category,0),
        	                                    ':',shop.id) ) sa_ids
	        FROM shop_catalogue_saved_cache scsc
	        JOIN saved_auctions as sa ON sa.id = scsc.saved_id 
	        JOIN shop ON sa.username = shop.username AND sa.siteid = shop.siteid   
	        WHERE scsc.saved_id in (".implode(",", $this->sa_ids_filtered).") 
	        GROUP BY scsc.cat_id
	    ";
//        echo $sql.PHP_EOL; // @debug
        $this->cats_SA = $dbr->getAssoc($sql);
        if (!$this->checkPearError($this->cats_SA)) return false;
        if ($this->isLogging) echo "Step 4. Cat's with assigned SA's have downloaded': ".count($this->cats_SA).PHP_EOL;
//        print_r( array_slice($this->cats_SA, 0, 2, true) ); // @debug

        // explode SA list
        $this->cats_SA = array_map( fn($val) =>
                            array_map( fn($vval) =>

                                // ... it can add what needed to evaluate in formula
                                array_combine(["sa_id","main_cat_id","shop_id"], explode(":", $vval))

                                , explode(",", $val)
                            )
                        , $this->cats_SA);

//        print_r( array_slice($this->cats_SA, 0, 2, true) ); // @debug
        /********************************************************
        * STEP 5, get list of SA's for each category
        ********************************************************/
        $sql = "
            SELECT concat(scsc.cat_id, '_', shop.id) k
                 , group_concat(sa.id ) 
            FROM shop_catalogue_saved_cache scsc
            JOIN saved_auctions sa ON sa.id = scsc.saved_id
            JOIN shop on shop.username = sa.username AND shop.siteid = sa.siteid
            WHERE 
                    sa.inactive = 0  
                    AND cat_id in(". implode(",", array_keys($this->cats_SA) ).")
            GROUP BY shop.id, scsc.cat_id
        ";
//        echo $sql.PHP_EOL; // @debug
        $this->cats_OF = $dbr->getAssoc($sql);
        if (!$this->checkPearError($this->cats_OF)) return false;
        if ($this->isLogging) echo "Step 5. SA list has downloaded for each needed categories and shops: ".count($this->cats_OF).PHP_EOL;
//        print_r( array_slice($this->cats_OF, 0, 150, true) ); // @debug

        // explode ordering factor
        $this->cats_OF = array_map(fn($val) => explode(",", $val), $this->cats_OF);

        /********************************************************
        * STEP 6, get list of teasers for each SA
        ********************************************************/
        $sql = "
            SELECT sa.id as sa_id 
                    , saved_pic.doc_id
                    , saved_pic.alt_teaser_color
                    , saved_pic.alt_teaser_whitesh
                    , saved_pic.alt_teaser_whitenosh
                    , saved_pic.img_type
            FROM saved_pic 
            JOIN saved_auctions sa ON sa.master_sa = saved_pic.saved_id OR sa.id = saved_pic.saved_id
            WHERE (alt_teaser_color>0 OR alt_teaser_whitesh>0 OR alt_teaser_whitenosh>0 OR img_type = \"".static::PRIMARY_IMG_TYPE."\")
                    AND sa.id in (".implode(",", $this->sa_ids_filtered).") 
        ";
//        echo $sql.PHP_EOL; // @debug
        $teasers_db = $dbr->getAll($sql);
        if (!$this->checkPearError($teasers_db)) return false;

        // prepare index array for found teasers by sa_id
        $this->teasers_data = [];
        foreach ($teasers_db as $item) {
            $doc_type = $item->img_type==static::PRIMARY_IMG_TYPE ? static::PRIMARY_IMG_TYPE :
                ($item->alt_teaser_color==1 ? "color" :
                    ($item->alt_teaser_whitesh==1 ? "whitesh" :
                        ($item->alt_teaser_whitenosh==1 ? "whitenosh" : "")));
            if ($doc_type == "") {
                if ($this->isLogging) echo "WARNING: wrong data structure on sa_id=" . $item->sa_id . PHP_EOL;
                continue;
            }
            $this->teasers_data[ $item->sa_id ][] = ["doc_id" => $item->doc_id, "doc_type" => $doc_type ];
        }


        echo "Step 6. Teaser's data has uploaded for found SA's': ".count($this->teasers_data).PHP_EOL;

//        print_r( array_slice($this->teasers_data, 0, 2, true) ); // @debug

        if ($this->isLogging) echo "--- ".get_class($this).". Finish preloading".PHP_EOL;

        return true;
    } // preloadDataNewArticles

    /**
     * generate an OF for cat_id, sa_id, doc_id and doc_type
     * @param string $cheatFormula
     * @param object $calcedDataProvider use $calcedDataProvider->getDataByKey($key)     *
     * @return \Generator
     */
    public function evalFormulaNewArticles($cheatFormula = null, $calcedDataProvider = null) : \Generator
    {
        $this->calcedDataProvider = $calcedDataProvider;
        //iteration by categories
        foreach($this->cats_SA as $cat_id => $cats_SA_array)
        {
            //iteration by SA assigned to this category
            foreach ($cats_SA_array as $sa_item)
            {
                $sa_id = $sa_item["sa_id"];

                // for calculate function call
                $sa_item['cat_id'] = $cat_id;

                // stop it there are too many errors
                if ($this->formula_errors > static::MAX_FORMULA_ERRORS) {
                    if ($this->isLogging) echo "Too many formula errors. Stopped.".PHP_EOL;
                    return;
                }

                $formula = $formulaWithVars = $this->sa_data[ $sa_id ]["of_formula"];
                if (!isset( $formula ) ) {
                    $this->formula_errors++;
                    if ($this->isLogging) "Not found formula for sa_id={$sa_id}".PHP_EOL;
                    continue;
                }

                // find and replace all variables in the formula
                // *************************************************
                if (preg_match_all("/\[(.*?)\]/i", $formula, $matches_var))
                {
                    // iteration by variables
                    foreach( array_unique( $matches_var[1] ) as $variable)
                    {
                        if ( preg_match("/OF_top_(\d*)_main_category/", $variable, $matches_X) ) {
                            /*
                             * [OF_top_X_main_category] variable
                            */
                            // to protect of preg_match behavior
                            // we have as result in $matches_X : array if many values, and a string if one value, but default behavior is array always
                            $sa_item['X'] = is_array($matches_X[1]) ? $matches_X[1][0] : $matches_X[1];
                            $value = $this->OF_top_X_main_category($sa_item);
//                            if($this->logEval) echo "CALC variable [{$variable}] data:"
//                                                    .str_replace(PHP_EOL, "", print_r($sa_item, true)).PHP_EOL;
                            $formula = str_replace("[OF_top_" . $sa_item['X'] . "_main_category]", $value, $formula);
                        } elseif ( preg_match("/setPosition_(\d*)_onEachCategory/", $variable, $matches_X) ) {
                            /*
                             * [setPosition_X_onEachCategory] variable
                            */
                            // to protect of preg_match behavior
                            // we have as result in $matches_X : array if many values, and a string if one value, but default behavior is array always
                            $sa_item['X'] = is_array($matches_X[1]) ? $matches_X[1][0] : $matches_X[1];
                            $value = $this->setPosition_X_onEachCategory($sa_item);
                            $formula = str_replace("[setPosition_" . $sa_item['X'] . "_onEachCategory]", $value, $formula);
                        } elseif ( false ) {

                            // @todo it is a place to add another variables

                        } else {
                            $this->formula_errors++;
                            if ($this->isLogging) echo "WARNING: can not calculate variable=[{$variable}]".PHP_EOL;
                        }

                    }
                }

                // eval formula
                try {

                    eval("\$formula_value = " . $formula . ";");

                } catch (\Throwable $ex) {
                    $this->formula_errors++;
                    if ($this->isLogging) echo "ERROR: can not evaluate={$formulaWithVars} ".PHP_EOL;
                    $formula_value = -1;
                    unset($ex);
                }

                if($this->logEval) echo "EVAL for sa_id={$sa_id}, cat_id={$cat_id}, formula='{$formulaWithVars}': ".$formula." result=".$formula_value.PHP_EOL; // @debug

                if ( !isset( $this->teasers_data[$sa_id]) ) {
                    $this->formula_errors++;
                    if ($this->isLogging) echo "WARNING: not found teasers for sa_id={$sa_id} in the preloaded data".PHP_EOL;
                    continue;
                }

                // generate the same OF value for each teaser
                foreach($this->teasers_data[$sa_id] as $teaser) {

                    yield [
                        "sa_id" => $sa_id,
                        "cat_id" => $cat_id,
                        "doc_id" => $teaser["doc_id"],
                        "doc_type" => $teaser["doc_type"],
                        "ordering_factor" => round( (float)$formula_value, static::OF_PRECISION)
                    ];

                } // $teaser
            } // $sa_id
        } // $cat_id

    } // evalFormula

    /**
     * calculate [availability_factor] value for SA using preloaded data
     * @param int $sa_id
     * @return int
     */
    public function availability_factor(int $sa_id)
    {
        $MAX_WEEKS = 52;
        if (!array_key_exists($sa_id, $this->sa_data)) return 0;
        $sa_item = $this->sa_data[$sa_id];

        // check existing data - just in case
        foreach( static::SAVED_AUCTIONS_AVAILABILITY_FIELDS as $field) {
            if (!array_key_exists($field, $sa_item)) return 0;
        }

        // extract data
        if ( isset($sa_item["shop_id"]) ) {
            $shop_id = $sa_item["shop_id"];
        } else {
            return 0;
        }
        if ( isset($this->ava_data[$shop_id]) ) {
            $ava_shop = $this->ava_data[$shop_id];
        } else {
            return 0;
        }

        // available logic
        // ********************************

        // find $ava_date - date when SA will be on stock
        if ($sa_item['available'] == 1) {
            // on stock
            return $ava_shop["on_stock_availability_factor"];
        } elseif ($sa_item['available_date_change_type'] == static::AVAILABLE_TYPE_CONTAINER) {
            // on container
            $available_container_id = $sa_item["available_container_id"];
            if (empty($available_container_id)) return 0;
            if (!($ava_date = $this->containers_available_date[$available_container_id])) return 0;
        } elseif ($sa_item["available_date_change_type"] == static::AVAILABLE_TYPE_WEEK) {
            // in 'available_weeks' weeks
            $weeks = (int)$sa_item["available_weeks"];
            if (is_nan($weeks) || $weeks<=0 || $weeks>$MAX_WEEKS) return 0;
            $ava_date = (new DateTime())->modify("+{$weeks} weeks")->format("Y-m-d");
        } elseif ($sa_item["available_date_change_type"] == static::AVAILABLE_TYPE_MANUAL) {
            $ava_date = $sa_item["available_date"];
        } else {
            return 0;
        }

        // find availability factor by $ava_date
        if (isset($ava_date)) {

            // find $days_interval
            $now = new DateTime(); // current date
            $date = DateTime::createFromFormat("Y-m-d", $ava_date); // convert $ava_date
            if ($now >= $date) {
                // wrong $ava_date value
                if ($this->isLogging) echo "WARNING: for SA#{$sa_id} availability_factor set 0 because available date {$ava_date} is less NOW.".PHP_EOL;
                return 0;
            }
            $days_interval = $now->diff($date)->days; // get diff using object DateInterval
//            echo "ava_date=", $ava_date, PHP_EOL, "interval=", $days_interval, ; // @debug

            // find availability factor by days interval and $ava_shop
            foreach($ava_shop["out_of_stock"] as $interval_data) {
                if ($days_interval >= $interval_data["days_from"]
                    && $days_interval <= $interval_data["days_to"])
                {
                    return $interval_data["availability_factor"];
                }
            } // foreach

        } // if $ava_date

        // if not found
        return 0;
    } // availability_factor

    private function OF_top_X_main_category(array $sa_item)
    {
        extract($sa_item);

        if (  !($X > 0 and $X < static::MAX_TOP_X) ) {
            $this->formula_errors++;
            if ($this->isLogging) echo "WARNING: X in [OF_top_X_main_category] must be > 0 and < ".static::MAX_TOP_X.PHP_EOL;
            return 0;
        }

        $cacheKey = $main_cat_id."_".$shop_id;
        $cat_OF = $this->cats_OF[ $cacheKey ];
        if ( !isset( $cat_OF ) ) {
            $this->formula_errors++;
            if ($this->isLogging) echo "WARNING(OF_top_X_main_category): For sa_id={$sa_id} not found OF in cat_id={$main_cat_id}, shop_id={$shop_id} in the preloaded data".PHP_EOL;
            return 0;
        }

        // calc OF for all SA in the category and save in the local cache
        if ( !isset( $this->cache_OF_top_X_main_category[ $cacheKey ] )) {
            $OFValues = [];
            foreach( $cat_OF as $sa_id ) {
                $OFValues[] = (float)\OrderingFactorHelper::getOrderingFactorByFormula(
                    $sa_id
                    , null
                    , $main_cat_id
                    , \OrderingFactorHelper::PRIMARY_DOC_TYPE
                    , false
                    , $this->calcedDataProvider->getDataByKey($sa_id)
                    , OrderingFactorHelper::REDIS_SUBKEY_TEASER
                );
            }
            rsort($OFValues);
            $this->cache_OF_top_X_main_category[ $cacheKey ] = array_slice($OFValues, 0, static::MAX_TOP_X);
        };
        $cat_OF_values = $this->cache_OF_top_X_main_category[ $cacheKey ];

        if ($this->logEval) echo "OF_top_X_main_category: in main category {$main_cat_id} on position {$X}"
                                ." founded value={$cat_OF_values[$X-1]}".PHP_EOL;

        // found SA in needed position
        if ( $X>count($cat_OF_values) ) {
            return end($cat_OF_values);
        } else {
            return $cat_OF_values[$X-1];
        }

    } // setPosition_X_onEachCategory

    private function setPosition_X_onEachCategory(array $sa_item)
    {
        extract($sa_item);

        if (  !($X > 0 and $X < static::MAX_TOP_X) ) {
            $this->formula_errors++;
            if ($this->isLogging) echo "WARNING: X in [setPosition_X_onEachCategory] must be > 0 and < ".static::MAX_TOP_X.PHP_EOL;
            return 0;
        }

        $cat_OF = $this->cats_OF[ $cat_id."_".$shop_id ];
        if ( !isset( $cat_OF ) ) {
            $this->formula_errors++;
            if ($this->isLogging) echo "WARNING(setPosition_X_onEachCategory): not found OF for cat_id={$cat_id} in the preloaded data".PHP_EOL;
            return 0;
        }

        if ( $X == 1 ) {
            if ( isset($cat_OF[0] ) ) {
                // up 10% for first value
                return $cat_OF[0]*1.1;
            } else {
                // if not OF at all
                return 1;
            }
        } else {
            if ( isset($cat_OF[$X-1]) ) {
                // average between X and prev value of X
                return ( $cat_OF[$X-2] + $cat_OF[$X-1] ) / 2;
            } else {
                if ( count( $cat_OF ) == 0) {
                    // if not OF at all
                    return 1;
                } else {
                    // less 10% last value
                    return end($cat_OF) * 0.9;
                }
            }
        }

    } // OF_top_X_main_category

}