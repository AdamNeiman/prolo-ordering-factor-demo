<?php

/**
 * @author Andrey Shtokalo
 * @descriprion working with CTR data and ordering factor calculate
 * @version 2.0
 */
use label\RedisProvider;

class OrderingFactorHelper
{
    const REDIS_KEY = "CTRData";
    const REDIS_KEY_FORMULA = "OF_FORMULA";
    const REDIS_SUBKEY_TEASER = "TE";
    const REDIS_SUBKEY_NEWARTICLES = "NA";
    const REDIS_SUBKEY_MULTISA = "MU";

    /**
     * there are: "color", "whitesh", "witenosh" for image doc_type
     * for OF logic is using additional type "primary" for default teaser
     */
    const PRIMARY_DOC_TYPE = "primary";

    const DEFAULT_DISPLAYING_TIME = 6;
    const LOG_NAME = "ordering_v2";
    const DEFAULT_TTL = 604800;

    // possible enum values for doc_type field of saved_pic and save_pic_ctr_clollect tables
    const DOC_TYPE_ENUM = ["color", "whitesh", "whitenosh"];

    // for convert functions
    const DOC_TYPES_CODES = [
        'primary'    => 0,
        'color'      => 1,
        'whitesh'    => 2,
        'whitenosh'  => 3
    ];

    //field names of calculated fields from auctions
    const AUCTIONS_DATA_AUCTIONS_LIST_SUFFIX = "_auctionsList";
    //field names of calculated fields from auctions for set zero if it is null
    const AUCTIONS_DATA_FIELDS_ZEROIFNULL = ["timed_brutto_income", "timed_purchases"];

    //field names which calculated per teaser
    const TEASER_DATA_FIELDS = ["teaser_clicks", "teaser_impressions", "teaser_ctr", "teaser_of"];
    const CTR_DATA_FIELDS = ["teaser_clicks", "teaser_impressions", "teaser_ctr"];
    const OF_DATA_FIELD = "teaser_of";

    // field names (short field names for radis to optimize memory)
    const IMPRESSION = 'i';
    const IMPRESSION_SKIPPED = 'j';
    const CLICK = 'c';
    const CLICK_SKIPPED = 'k';
    const REDIS_NAMES = [ 'i' => 'impression', 'j' => 'impression_skipped', 'c' => 'click', 'k' => 'click_skipped'];

    // cache
    public static $redis_data = [];

    /***********************************
     * teaser functions
     **********************************/

    public static function getDocTypeByDocCode(int $doc_code): string {
        return array_flip(static::DOC_TYPES_CODES)[$doc_code] ?? 'wrong';
    }

    public static function getDocCodeByDocType(string $doc_type): int {
        return static::DOC_TYPES_CODES[$doc_type] ?? 333;
    }

    /**
     * append into result array ordering factor value, key 'ordering_factor'
     * @param array $data
     * @param string $cat_id - from GET|POST param
     * @param bool $noClearCache - if true then it will not clear preloaded data
     * @return array|mixed
     */
    public static function appendOFToAjaxPrices($data, $cat_id, $noClearCache = false) {

        if (!isset($data)) return $data;

        $ids  = [];
        foreach($data as $complex_saved_id => $item) {
            [$saved_id] = explode("_", $complex_saved_id);
            $ids[] = $saved_id;
        }
       static::preloadOrderingFactorByFormula($ids, true);

        foreach($data as $complex_saved_id => $item)
        {
            [$saved_id, $doc_id, $doc_code] = explode("_", $complex_saved_id);

            if ( empty($doc_code) || empty($doc_id) ) {
                $doc_type = static::PRIMARY_DOC_TYPE;
                $doc_id = null;
            } else {
                $doc_type = static::getDocTypeByDocCode($doc_code);
            }

            $data[$complex_saved_id]['ordering_factor'] =  static::getOrderingFactorByFormula(
                $saved_id
                , $doc_id
                , $cat_id
                , $doc_type
               , true) ;
        } // foreach

        // clear cache
        if ( $noClearCache !== true ) static::clearRedisData();
        return $data;
    } // appendOFToAjaxPrices

    /**
     * append into result array ordering factor value, key 'ordering_factor'
     * default doc_type = 'primary'
     * @param array $offers array of stdClass
     * @param int $cat_id catalogue id
     * @param bool $noClearCache
     * @return array
     * @throws RedisException
     */
    public static function appendOFToOffersArray($offers, $cat_id, $noClearCache = false) {

        if (!isset($offers)) return $offers;

        $ids  = array_map(fn($v) => $v->saved_id, $offers);
        static::preloadOrderingFactorByFormula($ids, true);

        foreach($offers as $offer)
        {
            $doc_type = isset($offer->doc_type) ? $offer->doc_type : static::PRIMARY_DOC_TYPE;

            $offer->ordering_factor =  static::getOrderingFactorByFormula(
                $offer->saved_id
                , $offer->doc_id
                , $cat_id
                , $doc_type
                , true) ;
        } // foreach

        // clear cache
        if ( $noClearCache !== true ) static::clearRedisData();
        return $offers;
    } // appendOFToOffersArray

    /**
     * sort by ordering_factor field
     * @param array &$array array of stdClass || array of array
     * @return void
     */
    public static function sortByOF(array &$array) {
        $OF_KOEFF = pow(10, \OrderingFactorVariables::OF_PRECISION);
        usort($array, function ($a, $b) use($OF_KOEFF)
        {
            $aOF = $a instanceof stdClass ? $a->ordering_factor : $a["ordering_factor"];
            $bOF = $b instanceof stdClass ? $b->ordering_factor : $b["ordering_factor"];
            return $bOF * $OF_KOEFF - $aOF * $OF_KOEFF;
        });
    } // sortByOF

    /***********************************
     * auctions data fields block
     * [purchases][bruttoIncome]
     **********************************/

    /**
     * return true if $savedFields array included one or more fields for calc from auctions data
     * @param array $savedFields
     * @return bool
     */
    public static function isAuctionsDataFieldsInSavedFields(array $savedFields): bool
    {
        return count(array_diff(static::AUCTIONS_DATA_FIELDS_ZEROIFNULL, array_values($savedFields) ))
               < count(static::AUCTIONS_DATA_FIELDS_ZEROIFNULL);
    }

    /**
     * append auctions data fields values into $list array
     * @param array $list must each item as stdCalss, each item must have "sa_id" field
     * @param array $savedFields names of calced auction fields
     * @return array
     * @throws Exception
     */
    public static function appendAuctionsDataFiled(array $savedFields, array $list): array
    {
        $dbr = \label\DB::getInstance(\label\DB::USAGE_READ);

        // calc $ids for fast run query
        $ids = array_map(fn($v) => $v->sa_id, $list);
        if ( empty($ids) ) return $list;

        // calculate and save auction's data
        $auctionsData = [];
        $auctionsDataFields = [];
        foreach(static::AUCTIONS_DATA_FIELDS_ZEROIFNULL as $auctions_data_field) {
            if ( in_array($auctions_data_field, $savedFields) ) {
                $q = static::buildAuctionsDataQuery($ids, $auctions_data_field);
                $auctionsData[$auctions_data_field] = $dbr->getAssoc($q);
                if (\PEAR::isError($auctionsData[$auctions_data_field])) {
                    \ServerLogs::pearErrorLog('appendAuctionsDataFiled', $auctionsData[$auctions_data_field]);
                    return $list;
                }
                $auctionsDataFields[] = $auctions_data_field;
            }
        } // foreach

        // data iteration
        foreach ($list as $key=>$listItem) {

            // set values for $auctionsDataFields
            foreach($auctionsDataFields as $auctions_data_field) {
                // appending calculated data

                $auctions_data_field_auctionsList = $auctions_data_field.static::AUCTIONS_DATA_AUCTIONS_LIST_SUFFIX;

                if ( isset( $auctionsData[$auctions_data_field][$listItem->sa_id] ) ) {

                    $list[$key]->{$auctions_data_field} = $auctionsData[$auctions_data_field][$listItem->sa_id][$auctions_data_field];

                    // calculate auctions list
                    $auctionsList = $auctionsData[$auctions_data_field][$listItem->sa_id][$auctions_data_field_auctionsList];
                    $auctions = explode(",", $auctionsList);
                    $au_list = [];
                    foreach($auctions as $auction) {
                        [$auction_number, $txnid, $value] = explode("/", $auction);
                        $link = "/auction.php?number={$auction_number}&txnid={$txnid}";
                        $au_list[] = ["name" => $auction_number."/".$txnid. (isset($value) ? " (".round($value,2).")":"")
                                      , "link" => $link];
                    }
                    $list[$key]->{$auctions_data_field_auctionsList} = [
                        "total_auftrags" => count($auctions)
                        , "total_value"=>$list[$key]->{$auctions_data_field}
                        , "list"=>$au_list
                    ];

                } else {
                    $list[$key]->{$auctions_data_field} = 0;
                    $list[$key]->{$auctions_data_field_auctionsList} = ["total_auftrags" => 0, "total_value"=>0, "list"=>[]];
                }

            } // $AUCTIONS_DATA_FIELDS

        } // foreach $list

        return $list;
    } // appendAuctionsDataFiled

    /**
     * build sql query string to select auctions data fields
     *
     * it can be used:
     * FROM saved_auctions sa
     * LEFT JOIN ({buildAuctionsDataQuery}) auc ON auc.saved_id = sa.id
     *
     * @param array $ids SA's id
     * @param string $type should be one of static::AUCTIONS_DATA_FIELDS_ZEROIFNULL array
     * @return string
     */
    public static function buildAuctionsDataQuery(array $ids = [], string $type): string {
        if ($type == "timed_purchases") {
            $q = "
                SELECT timed_auc.saved_id
                       , SUM(timed_auc.timed_purchases) timed_purchases
                       , group_concat(timed_auc.complete_auction_number ORDER BY complete_auction_number) timed_purchases_auctionsList
                FROM (
                            SELECT auc.saved_id 
                                   , ROUND(SUM(o.quantity)/al.default_quantity,2) timed_purchases
                                   , CONCAT(auc.auction_number,'/', auc.txnid, '/', SUM(o.quantity)/al.default_quantity) complete_auction_number
                            FROM auction auc
                            JOIN orders o ON o.auction_number = auc.auction_number AND o.txnid = auc.txnid
                            JOIN offer_group og ON og.offer_id = auc.offer_id AND og.main = 1
                            JOIN article_list al ON	al.group_id = og.offer_group_id AND al.article_id = o.article_id 
                            -- for check condtion by time
                            JOIN invoice ON invoice.invoice_number = auc.invoice_number            
                            JOIN saved_auctions sa ON sa.id = auc.saved_id
                            JOIN shop ON shop.username = sa.username AND shop.siteid = sa.siteid 
                
                            WHERE o.manual = 0
                                  AND auc.deleted = 0
                                  AND al.inactive = 0
                                  AND invoice.invoice_date > DATE_ADD(CURRENT_TIMESTAMP, INTERVAL -shop.total_brutto_income_period DAY)
                                  AND invoice.invoice_date <= NOW()
                                  " . (!empty($ids) ? " AND auc.saved_id in (" . implode(",", $ids) . ")" : "") . "
                            GROUP by auc.auction_number, o.article_id
                ) as timed_auc
                GROUP by timed_auc.saved_id
            ";
        } elseif ($type == "timed_brutto_income") {
            $q = "
            SELECT unique_au.saved_id 
                   , IF(SUM(IFNULL(ac.brutto_per_article, 0))<0,0,SUM(IFNULL(ac.brutto_per_article, 0))) timed_brutto_income
                   , GROUP_CONCAT(DISTINCT CONCAT(unique_au.auction_number,'/', unique_au.txnid) ORDER BY unique_au.auction_number) timed_brutto_income_auctionsList
            FROM auction_calcs ac
            JOIN article_list al ON ac.article_list_id = al.article_list_id AND al.group_id
            JOIN article a ON al.article_id = a.article_id AND NOT a.admin_id
            JOIN (
                SELECT DISTINCT all_au.saved_id, all_au.auction_number, all_au.txnid
                FROM (
                SELECT auction.saved_id 
                       , auction.auction_number auction_number
                       , auction.txnid txnid
                FROM orders
                    JOIN auction ON auction.auction_number=orders.auction_number AND auction.txnid=orders.txnid
                    JOIN saved_auctions sa ON sa.id = auction.saved_id
                    JOIN shop ON shop.username = sa.username AND shop.siteid = sa.siteid 
                    JOIN invoice ON invoice.invoice_number = auction.invoice_number            
                WHERE NOT orders.manual 
                      AND invoice.invoice_date > DATE_ADD(CURRENT_TIMESTAMP, INTERVAL -shop.total_brutto_income_period DAY)
                      AND invoice.invoice_date <= NOW() 
                      " . (!empty($ids) ? " AND auction.saved_id in (" . implode(",", $ids) . ")" : "") . "
                UNION ALL
                SELECT auction.saved_id 
                       , auction.main_auction_number auction_number
                       , auction.main_txnid txnid
                FROM orders
                    JOIN auction ON auction.auction_number=orders.auction_number AND auction.txnid=orders.txnid
                    JOIN saved_auctions sa ON sa.id = auction.saved_id
                    JOIN shop ON shop.username = sa.username AND shop.siteid = sa.siteid 
                    JOIN invoice ON invoice.invoice_number = auction.invoice_number            
                WHERE NOT orders.manual 
                      AND invoice.invoice_date > DATE_ADD(CURRENT_TIMESTAMP, INTERVAL -shop.total_brutto_income_period DAY)
                      AND invoice.invoice_date <= NOW()
                      " . (!empty($ids) ? " AND auction.saved_id in (" . implode(",", $ids) . ")" : "") . "
                ) all_au
            ) unique_au ON unique_au.auction_number = ac.auction_number AND unique_au.txnid = ac.txnid 
            GROUP BY unique_au.saved_id 
        ";
        } else {
            $q = "";
        }
        return $q;
    } // buildAuctionsDataQuery

    /**
     * return true if $savedFields array included one or more fields for calc from auctions data
     * @param array $savedFields
     * @return bool
     */
    public static function isTeaserDataFieldsInSavedFields(array $savedFields): bool
    {
        return count(array_diff(static::TEASER_DATA_FIELDS , array_values($savedFields) ))
            < count(static::TEASER_DATA_FIELDS);
    }

    /**
     * append teaser ordering factor data into $list array
     * @param array $savedFields
     * @param array $list
     * @param $isAltTeaser
     * @return array
     * @throws RedisException
     */
    public static function appendTeaserDataFiled(array $savedFields, array $list, $isAltTeaser = false): array
    {
        $dbr = \label\DB::getInstance(\label\DB::USAGE_READ);
        global $smarty;

        // calc $ids for fast run query
        $ids = array_map(fn($v) => $v->sa_id, $list);
        if ( empty($ids) ) return $list;

        $OFVariables = new \OrderingFactorVariables(false);
        $OFVariables->setSAids($ids);
        $OFVariables->step_allTeasers();
        $isCTRData = !empty(array_intersect(static::CTR_DATA_FIELDS, $savedFields));
        if ( $isCTRData ) {
            $OFVariables->step_CTR();
        }

        // get all doc_id extensions for image urls
        $doc_ids = [];
        foreach($OFVariables->teasers_data as $sa_teasers) {
            foreach ($sa_teasers as $teaser) {
                $doc_ids[] = $teaser["doc_id"];
            }
        }
        $doc_ids = array_unique($doc_ids);
        $q = "SELECT doc_id k, ext_color color, ext_whitesh whitesh, ext_whitenosh whitenosh 
              FROM saved_pic
              WHERE doc_id in (".implode(",", $doc_ids).")";
        $ext_data = $dbr->getAssoc($q);
        if (\PEAR::isError($ext_data)) {
            \ServerLogs::pearErrorLog(__METHOD__, $ext_data);
            return $list;
        }

        // preload data from Redis for speed up function 'smarty_function_imageurl'
        \Image::preloadVersions($doc_ids);

        // set images urls for teasers
        $fields = array_intersect(static::TEASER_DATA_FIELDS , array_values($savedFields) );
        foreach($OFVariables->teasers_data as $sa_id => $sa_teasers) {
            foreach($sa_teasers as $ind_teaser => ["doc_id" => $doc_id, "doc_type" => $doc_type]) {

                // image types
                $types = $doc_type== "primary" ? ["color", "whitesh"] : [$doc_type];
                $images = [];
                $clicks = 0; $impressions = 0;
                foreach ($types as $type) {

                    // find extension
                    if ( isset($ext_data[$doc_id] ) && isset($ext_data[$doc_id]->{$type}) ) {
                        $ext = $ext_data[$doc_id]->{$type};
                        echo  $ext; die;
                    } else {
                        $ext = "jpg";
                    }

                    // add image
                    $images[] = smarty_function_imageurl([
                        'src' => 'sa',
                        'picid' => $doc_id,
                        'x' => 99,
                        'type' => $type,
                        'ext' =>$ext,
                    ], $smarty);

                    // add CTR data
                    if ( $isCTRData ) {
                        if ( isset($OFVariables->teasers_variables[ $sa_id ])
                             &&  isset($OFVariables->teasers_variables[ $sa_id ][ $doc_id ])
                             &&  isset($OFVariables->teasers_variables[ $sa_id ][ $doc_id ][ $type ]) ) {
                            $clicks += $OFVariables->teasers_variables[ $sa_id ][ $doc_id ][ $type ][ "clicks" ];
                            $impressions += $OFVariables->teasers_variables[ $sa_id ][ $doc_id ][ $type ][ "impressions" ];
                        }
                    }

                } // $types
                $OFVariables->teasers_data[$sa_id][$ind_teaser]["images"] = $images;

                // set CTR data
                if ( $isCTRData ) {
                    $OFVariables->teasers_data[$sa_id][$ind_teaser]["teaser_clicks"] = $clicks;
                    $OFVariables->teasers_data[$sa_id][$ind_teaser]["teaser_impressions"] = $impressions;
                    if ($impressions == 0) {
                        $OFVariables->teasers_data[$sa_id][$ind_teaser]["teaser_ctr"] = 0;
                    } else {
                        $OFVariables->teasers_data[$sa_id][$ind_teaser]["teaser_ctr"]
                            = round($clicks / $impressions, \OrderingFactorVariables::OF_PRECISION + 1);
                    }
                }

            } // $sa_teasers
        } // $OFVariables->teasers_data

        // create new $list to return for add new rows
        $retList = [];
        foreach ($list as $listItem) {
            $sa_id = $listItem->sa_id;

            $teasers = $OFVariables->teasers_data[$sa_id];
            if (!isset($teasers)) continue; // just in case

            foreach($teasers as $i => $teaser) {
                if ($i==0) { // primary teaser
                    $toAdd = $listItem;
                } else { // alt teaser
                    $toAdd = clone $listItem;
                }

                // set value
                foreach ($fields as $field) {
                    if ($field == static::OF_DATA_FIELD) {
                        $teaser["value"] = static::getOrderingFactorByFormula($sa_id, $teaser["doc_id"], null, $teaser["doc_type"]);
                    } elseif (in_array($field, static::CTR_DATA_FIELDS) ) {
                        $teaser["value"] = $teaser[$field];
                    }
                    $listItem->{$field} = $teaser;
                }

                $retList[] = $toAdd;
                if  (!$isAltTeaser) break; // primary teaser only
            }

        } // $list

        return $retList;
    } // appendTeasersDataFiled

    /***********************************
     * CTR block
     **********************************/

    /**
     *
     * @return CRT session live time in Hours
     * @param string $type possible values: 'seconds'|'sec' - in seconds
    null|'hours'    - in hours
     */
    public static function getCTRSessionLiveTime($type='hours') {
        $CRTSessionLiveTimeH = APPLICATION_ENV === 'production' ? 2 : 1;
        if ($type == 'seconds' || $type == 'sec') {
            return $CRTSessionLiveTimeH*60*60;
        } else {
            return $CRTSessionLiveTimeH;
        }
    } // getCRTSessionLiveTime

    /**
     *
     * @return dispaying time (impression time) in seconds
     */
    public static function getDisplayingTime() {
        global $shopCatalogue;
        if ( isset($shopCatalogue) ) {
            return (int)$shopCatalogue->_shop->displaying_time;
        } else {
            return static::DEFAULT_DISPLAYING_TIME;
        }
    }

    /**
     * @param array ctr_data - array of ctr, sample
     * [ => ['saved_id' => 522,
    'doc_id' => 765043,                               // mandatory
    'doc_type' => whitesh,                            // mandatory
    'collect_type' => 'click',                        // or 'impression', mandatory
    'time' => 1676610285,                             // not mandatory, will set to current system time if not sent
    'session' => '0601a746ce5664f562bbd9dc135b2a84'], // not mandatory
    [...]]
     */
    public static function saveCTRData( $ctr_data ) {
        $redis_dt = RedisProvider::getInstance(RedisProvider::USAGE_CACHE);
        $session_live_time = static::getCTRSessionLiveTime('sec');
        $dispaying_time = static::getDisplayingTime();
        $default_session = $_COOKIE['PHPSESSID'];
        $current_time = time();

        foreach($ctr_data as $item) {
            // mandatory fields
            $doc_id = (int)$item['doc_id'];
            $doc_type = $item['doc_type'];
            $saved_id = (int)$item['saved_id'];
            $collect_type = $item['collect_type'];
            if (!$doc_id || !$doc_type || !$collect_type) continue;

            // not mandatory fields
            // $time = $item['time'] ?: $current_time; // deprecated
            /*
                it catches many wrong timestamps from frontend script
                now, it always uses server timestamp
            */
            $time = $current_time;

            $session = $item['session'] ?: $default_session;

            $redis_key = $doc_id . '_' . $doc_type . '_' . $saved_id;
            $data = $redis_dt->hGet(static::REDIS_KEY, $redis_key);

            if ($data) {
                $data = json_decode($data, true);
            } else {
                $data = [];
            }

            if ( isset( $data[$session] ) ) { // exist session
                /**
                 * increment session data
                 */
                if ($collect_type == 'impression') {
                    /**
                     * impression logic
                     */
                    if ( count($data[$session][static::IMPRESSION]) == 0 || ($time - end($data[$session][static::IMPRESSION]) ) > $session_live_time ) {
                        $data[$session][static::IMPRESSION][] = $time;
                    } else {
                        $data[$session][static::IMPRESSION_SKIPPED][] = $time;
                    }
                } else {
                    /**
                     * click logic
                     */
                    if ( count($data[$session][static::CLICK]) == 0 || ($time - end($data[$session][static::CLICK])) > $session_live_time ) {
                        $data[$session][static::CLICK][] = $time;

                        // add increment for impression logic when click collect
                        if ( count($data[$session][static::IMPRESSION]) == 0 || ($time - end($data[$session][static::IMPRESSION])) >= $dispaying_time ) {
                            $data[$session][static::IMPRESSION][] = $time;
                        }

                    } else {
                        $data[$session][static::CLICK_SKIPPED][] = $time;
                    }
                }

            } else {
                /**
                 * new session
                 */

                if ($collect_type == 'impression') {
                    /**
                     * impression logic
                     */
                    $data[ $session ] = [ static::IMPRESSION => [$time],
                        static::CLICK => [],
                        static::IMPRESSION_SKIPPED => [],
                        static::CLICK_SKIPPED => []
                    ];
                } else {
                    /**
                     * click logic
                     */
                    $data[ $session ] = [static::IMPRESSION => [$time],
                        static::CLICK => [$time],
                        static::IMPRESSION_SKIPPED => [],
                        static::CLICK_SKIPPED => []
                    ];

                }

            }

            // set data into redis db
            $data_json = json_encode($data);

            // native call redis function depricated
            $result = $redis_dt->hSet(static::REDIS_KEY, $redis_key,  $data_json);

            /**
             * important note
             * 0 is valid value for $result it just means that key is exist
             */
            if ($result===false) {
                \ServerLogs::createLogFile( static::LOG_NAME, "redis_error", "Save error. Data length="
                    .strlen($data_json)."; redis_key=".$redis_key."; value=".substr($data_json,0,50)."...");
            }
        } // foreach $ctr_data

    } // saveCRTData

    /***********************************
     * Availability Factor block
     **********************************/

    /**
     * update table `shop_availability_factor` for $shop_id
     * $post_data = [
     *  'availability_factor_from' => [int ...],
     *  'availability_factor_to' => [int ...],
     *  'availability_factor_not_on_stock' => [float ...]
     * ]
     * @param int $shop_id
     * @param array $post_data
     * @return void
     * @throws Exception
     */
    public static function saveShopAvailability(int $shop_id, array $post_data)
    {

        $db = \label\DB::getInstance(\label\DB::USAGE_WRITE);

        // load current data
        $current_data = static::getShopAvailability($shop_id);

        $sql_arr = [];
        // insert and update sql
        foreach($post_data["availability_factor_not_on_stock"] as $index => $availability_factor) {
            $days_from = $post_data["availability_factor_from"][$index];
            $days_to = $post_data["availability_factor_to"][$index];

            $isUpdated = false;
            foreach($current_data as $current_data_item) {
                if ($current_data_item->updated == true) continue;
                if ($current_data_item->days_from == $days_from
                   &&  $current_data_item->days_to == $days_to)
                {
                    $isUpdated = true;
                    $current_data_item->updated = $isUpdated;
                    if ($current_data_item->availability_factor != $availability_factor) {
                        $sql_arr[] = "UPDATE shop_availability_factor 
                                      SET availability_factor = {$availability_factor}
                                      WHERE id = {$current_data_item->id}";
                    }
                }
            }

            // new item
            if (!$isUpdated) {
                $sql_arr[] = "INSERT INTO shop_availability_factor 
                              (shop_id, days_from, days_to, availability_factor )
                             VALUES ({$shop_id}, {$days_from}, {$days_to}, {$availability_factor})";
            }

        }

        // delete sql
        $to_delete_ids = [];
        foreach($current_data as $current_data_item) {
            if ($current_data_item->updated !== true) $to_delete_ids[] = $current_data_item->id;
        }
        if (!empty($to_delete_ids))
        {
            $sql_arr[] = "DELETE FROM shop_availability_factor WHERE id in (".implode(",", $to_delete_ids).")";
        }

        /*var_dump($sql_arr);
        die;*/

        // execute query
        foreach($sql_arr as $sql)
        {
            $result = $db->query($sql);
            if (\PEAR::isError($result)) {
                \ServerLogs::pearErrorLog('saveShopAvailability', $result);
            }
        }

    }

    /**
     * load all availability factor data from `shop_availability_factor` table for the $shop_id
     * @param int $shop_id
     * @return array
     * @throws Exception
     */
    public static function getShopAvailability(int $shop_id)
    {
        $dbr = \label\DB::getInstance(\label\DB::USAGE_READ);

        // load current data
        $sql = "
            SELECT *
            FROM shop_availability_factor
            WHERE shop_id = {$shop_id}
            ORDER BY days_from
        ";
        $current_data = $dbr->getAll($sql);
        if (\PEAR::isError($current_data)) {
            \ServerLogs::pearErrorLog('saveShopAvailability', $current_data);
        }
        return $current_data;
    }

    /**
     * copy availability data from $shop_id to all others shops
     * @param int $shop_id
     * @return int
     * @throws Exception
     */
    public static function copyShopAvailabilityToAllShops(int $shop_id)
    {
        if (empty($shop_id)) return 0; // just in case

        $dbr = \label\DB::getInstance(\label\DB::USAGE_READ);
        $db = \label\DB::getInstance(\label\DB::USAGE_WRITE);

        $sql_arr = [];
        // load data by $shop_id
        $data = static::getShopAvailability($shop_id);

        // load all shop's id
        $q = "SELECT id FROM shop WHERE id <> {$shop_id}";
        $shops = $dbr->getCol($q);
        if (\PEAR::isError($shops)) {
            \ServerLogs::pearErrorLog('copyShopAvailabilityToAllShops', $shops);
            return 0;
        }

        // delete existing values
        $sql_arr[] = "DELETE FROM shop_availability_factor WHERE shop_id <> {$shop_id}";

        // insert copy values
        foreach($shops as $shops_item_id) {
            foreach ($data as $data_item) {
                $sql_arr[] = "INSERT INTO shop_availability_factor 
                           (shop_id, days_from, days_to, availability_factor )
                          VALUES ({$shops_item_id}, {$data_item->days_from}, {$data_item->days_to}, {$data_item->availability_factor})";
            }
        }

        // execute query
        foreach($sql_arr as $sql)
        {
            $result = $db->query($sql);
            if (\PEAR::isError($result)) {
                \ServerLogs::pearErrorLog('copyShopAvailabilityToAllShops', $result);
                return 0;
            }
        }
        return count($shops);
    } // copyShopAvailabilityToAllShops

    /**
     * for saved_details controller, append [availability_factor] values into $list array
     * using OrderingFactorVariables class in silent mode (without log messages)
     * @param array $list
     * @return array
     * @throws Exception
     */
    public static function appendAvailabilityFactorField(array $list): array
    {
        // calc $ids for fast run query
        $ids = array_map(fn($v) => $v->sa_id, $list);
        if ( empty($ids) ) return $list;


        // preload data for calc variable
        $OFVariables = new \OrderingFactorVariables(false);
        if ($OFVariables->preloadDataTeaser($ids) === false) {
            return $list;
        };

        // append availability_factor data
        foreach ($list as $key=>$listItem) {
            $list[$key]->availability_factor = $OFVariables->availability_factor($listItem->sa_id);
        }

        return $list;
    }

    /***********************************
     * CACHE block
     **********************************/
    /**
     * empty static cache data
     */
    public static function clearRedisData() {
        static::$redis_data = [];
    }

    /**
     * preload data into static array, skipping already preloaded
     * and extract data
     * @param array $ids
     * @return void
     * @throws RedisException
     */
    private static function OFpreloadIntoRedisDataArray(array $ids) {
        $redis = RedisProvider::getInstance(RedisProvider::USAGE_READ_CACHE, RedisProvider::READ_MODE);

        // check if already preloaded
        $ids_toPreload = [];
        foreach ($ids as $saved_id) {
            if ( !isset(static::$redis_data[$saved_id]) ) $ids_toPreload[] = $saved_id;
        }
        if (empty($ids_toPreload)) return;

        // get all data
        $redis_data_str= $redis->hmGet(static::REDIS_KEY_FORMULA, $ids_toPreload);
        // extract data
        foreach($redis_data_str as $saved_id => $data_str) {
            static::$redis_data[$saved_id] = json_decode($data_str, true);
        }
    } // OFpreloadIntoRedisDataArray

    /**
     * preload data for function 'getOrderingFactorByFormula'
     * data uses automatically in function 'getOrderingFactorByFormula'
     * @param array $ids
     * @param bool $useMultiSALogic
     * @return void
     * @throws RedisException
     */
    public static function preloadOrderingFactorByFormula(array $ids, bool $useMultiSALogic = false) {

        static::$redis_data = []; // clear just in case

        // STEP1 preload by $id
        static::OFpreloadIntoRedisDataArray($ids);

        // STEP2 preload by multiSA array included in the data
        if ($useMultiSALogic) {

            // get all multiSA ids
            $multiSA_ids = [];
            foreach(static::$redis_data as $data) {
                if ( isset($data[static::REDIS_SUBKEY_MULTISA]) ) {
                    $multiSA_ids = array_merge($multiSA_ids, $data[static::REDIS_SUBKEY_MULTISA]);
                }
            } // foreach
            $multiSA_ids = array_values(array_unique($multiSA_ids));

            // preload multiSA too
            if (!empty($multiSA_ids)) {
                static::OFpreloadIntoRedisDataArray($multiSA_ids);
            }
        }
    } // preloadOrderingFactorByFormula


    /***********************************
     * Ordering Factor block
     **********************************/

    /**
     * @return array of all hash keys (saved_id) in the Redis DB
     * @throws RedisException
     */
    public static function getOrderingFactorSaIds():array {
        $redis = RedisProvider::getInstance(RedisProvider::USAGE_CACHE, RedisProvider::READ_MODE);
        $redis_data = $redis->hGetAll(static::REDIS_KEY_FORMULA);
        return array_keys($redis_data);
    }

    /**
     * @param array $saIds array of saved_id
     * @param $groupBy field in the redis db to group by it
     * @param $sort ASC|DESC, default = "" none
     * @return array|void indexed and sorted by field $groupBy array
     * @throws RedisException
     */
    public static function getOrderingFactorByFormulaForSaIds(array $saIds, $groupBy="of", $sort="") {
        $redis = RedisProvider::getInstance(RedisProvider::USAGE_CACHE, RedisProvider::READ_MODE);
        $REDIS_FIELDS = ["of", "di", "dt", "ci"];
        if (!in_array($groupBy, $REDIS_FIELDS)) return;
        // make of array
        $of = [];
        $subKeys = [static::REDIS_SUBKEY_NEWARTICLES, static::REDIS_SUBKEY_TEASER];
        foreach($saIds as $sa_id) {
            $data_str = $redis->hGet(static::REDIS_KEY_FORMULA, $sa_id);
            $data = json_decode($data_str, true);

//            print_r($data);

            foreach($subKeys as $subKey) {
                foreach($data[$subKey] as $data_item) {
                    $ofKey = (string)$data_item[$groupBy];
                    foreach ($REDIS_FIELDS as $field ) {
                        $of[ $ofKey ][ $field ][] = $data_item[$field];
                    }
                    $of[ $ofKey ]["sa"][] = $sa_id.(isset($data[static::REDIS_SUBKEY_MULTISA]) ? '_mu': '');
                    $of[ $ofKey ]["t"][] = $data_item["di"].":".$data_item["dt"];
                    $of[ $ofKey ]["sk"][] = $subKey;
                }
            }

        }
        // make unique
        foreach($of as $ofKey => $ofValue) {
            foreach ($ofValue as $ofValueKey => $ofValueValue) {
                $of[ $ofKey ][ $ofValueKey ] = array_unique($ofValueValue);
            }
        }
        if (!empty($sort)) {
            ksort($of);
            if ($sort=="DESC") $of = array_reverse($of, true);
        }

        return $of;
    }

    /**
     * return array on all unique teasers for sa_id which saved in the redis DB by OF hash key
     * @param int $sa_id
     * @return array return format: ["primary:docid","whitesh:docid"]
     * @throws RedisException
     */
    public static function getTeasersFromOrderingFactorData(int $sa_id) {
        $redis = RedisProvider::getInstance(RedisProvider::USAGE_READ_CACHE, RedisProvider::READ_MODE);
        $data_str = $redis->hGet(static::REDIS_KEY_FORMULA, $sa_id);
        if ($data_str === false) return [];

        $data = json_decode($data_str, true);

        // init subkeys of data to iterate
        $subKeys = [static::REDIS_SUBKEY_NEWARTICLES, static::REDIS_SUBKEY_TEASER];
        $teasers = [];
        foreach($subKeys as $subKey) {
            foreach ($data[$subKey] as $item) {
                $teaser = $item["di"].":".$item["dt"];
                if (!in_array($teaser, $teasers)) $teasers[] = $teaser;
            }
        }
        return $teasers;
    } // getTeasersFromOrderingFactorData

    /**
     * @param int $sa_id
     * @param int|null $doc_id
     * @param int|null $cat_id
     * @param string $doc_type
     * @param bool $useMultiSA  use multiSA logic: if SA has multiSA that OF = maximum of multiSA values
     * @return float
     * @throws RedisException
     */
    public static function getOrderingFactorByFormula(int $sa_id
                                                    , int $doc_id = null
                                                    , int $cat_id = null
                                                    , string $doc_type = self::PRIMARY_DOC_TYPE
                                                    , bool $useMultiSALogic = false
                                                    , $redis_data = null
                                                    , $subKey_param = null)
    {
        // default value
        $OF = 0;

        // use preloaded data
        if ( isset( static::$redis_data[$sa_id] ) ) {
            $redis_data = static::$redis_data[$sa_id];
        }

        // init data for $OF search
        if ( is_null($redis_data) ) {
            // using READ_MODE
            $redis = RedisProvider::getInstance(RedisProvider::USAGE_READ_CACHE, RedisProvider::READ_MODE);

            $data_str = $redis->hGet(static::REDIS_KEY_FORMULA, $sa_id);
            if ($data_str === false) return 0;

            $data = json_decode($data_str, true);
        } else {
            $data = $redis_data;
        }

        // init subkeys of data to iterate
        $subKeys = [static::REDIS_SUBKEY_NEWARTICLES, static::REDIS_SUBKEY_TEASER];
        // if it needs to force use a specific key
        if ( in_array($subKey_param, $subKeys) ) $subKeys = [$subKey_param];

        foreach($subKeys as $subKey) {

            // if it is not defined key - just use next key
            if (!isset($data[$subKey])) continue;

            // main iterator
            foreach ($data[$subKey] as $item) {

                // filter by $doc_id
                if (!is_null($doc_id)) {
                    if ($item["di"] != $doc_id) continue;
                }

                // filter by $doc_type
                if ($item["dt"] != $doc_type) continue;

                // filter by $cat_id
                if (!is_null($cat_id)) {
                    if ($item["ci"] == $cat_id) {
                        // 100% equal
                        $OF = $item["of"];
                        break;
                    }
                }

                // @todo
                // now cat logic for OF is soft, it does not need 100 equal by cat_id, we can use just maximum OF of all cats

                if ($OF < $item["of"]) $OF = $item["of"];

            } // $data

            // stop search
            if ($OF > 0) break;

        } // $subkeys

        /**
         * https://trello.com/c/M1aQcCx2/18993-danuta-dolega-multi-sa-take-the-highest-ordering-factor-no-1603
         * take the highest OF for Multi SA
         */
        if ($useMultiSALogic === true && isset($data[static::REDIS_SUBKEY_MULTISA]) ) {

            foreach($data[static::REDIS_SUBKEY_MULTISA] as $item_sa_id) {
                $OF = max($OF, static::getOrderingFactorByFormula($item_sa_id,null, $cat_id, static::PRIMARY_DOC_TYPE, false));
            }

        } // if $useMultiSALogic

        return (string)$OF;
    } // getOrderingFactorByFormula

}
