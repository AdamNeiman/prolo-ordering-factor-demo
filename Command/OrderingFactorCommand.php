<?php

/**
 * @author Andrey Shtokalo
 * @version 2023-03-07
 * 
 * ordering factor collecting and calculate data
 * working with CTR data
 * CTR = click/impression 
 */

namespace label\RecacheShop;

use label\DB;
use helpers\SQLHelper;
use label\RedisProvider;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;


/**
 *
 */
class OrderingFactorCommand extends Command
{
    use \checkPearErrorTrait;

    /**
     * @var InputInterface
     */
    private $input;

    /**
     * @var OutputInterface
     */
    private $output;

    /**
     * Current shop object
     *
     * @var \Shop
     */
    private $_shop;

    /**
     * @var array
     * list of sa's id from option --id-sa
     */
    private $sa_ids;

    /**
     * @var array
     * to save into the Redis DB
     */
    private array $redis_data;

    /**
     *
     * @var Array
     */
    private $launcher_log;

	/**
	* @const string
	*/
    const LOG_NAME = "OrderingFactor";

    /**
     * Configure the current command
     */
    protected function configure()
    {
        $this
            ->setName('recache:orderingFactor')
            ->setDescription('Move CTR from redis to DB and calculate ordering factor')
            ->addOption(
                'savefromredis', null, InputOption::VALUE_NONE, 'Save CTR Data from Redis into mySQL'
            )
			->addOption(
                'maxdate', null, InputOption::VALUE_OPTIONAL, 'Max date of redis CTR data to move in mySQL'
            )
            ->addOption(
                'cheatFormula', null, InputOption::VALUE_REQUIRED, 'Change(cheat) formula in the settings'
            )
            ->addOption(
                'brutto', null, InputOption::VALUE_NONE, 'Recalculate brutto'
            )
            ->addOption(
                'teaserof', null, InputOption::VALUE_NONE, 'Recalculate ordering for teaserOF'
            )
			->addOption(
                'ordering', null, InputOption::VALUE_NONE, 'Recalculate ordering'
            )
 			->addOption(
                'formula', null, InputOption::VALUE_NONE, 'Recalculate new articles by formula'
            )
            ->addOption(
                'id-sa', null, InputOption::VALUE_IS_ARRAY | InputOption::VALUE_REQUIRED, 'recache only that sa id'
            )
			->setHelp('Example debug' . PHP_EOL . 'To debug SA use next command: php console.php recache:orderingFactor');
    }
    /** @noinspection PhpMissingParentCallCommonInspection */

    /**
     * Run the current command
     * @param InputInterface $input
     * @param OutputInterface $output
     * @return int
     * @throws \Exception
     */
    protected function execute(InputInterface $input, OutputInterface $output)
    {
		$st = time();
        
		$this->input = $input;
        $this->output = $output;
        $this->loadSaIds();
		echo "Start OrderingFactorCommand at ".date("Y-m-d h:i:s")." APPLICATION_ENV=\"".APPLICATION_ENV."\"".PHP_EOL;
		/**
		Option 1, save data from REDIS to mySQL and clear REDIS
		*/
        if ($this->input->getOption('savefromredis')) {
            $this->processSaveFromRedis();
        }

		/**
		Option 2, calculate brutto
		*/  
		if ($this->input->getOption('brutto')) 
		{
			$this->calculateBrutto();
		}
		
		/**
		Option 3, calculate ordering
		*/  
		if ($this->input->getOption('ordering')) 
		{
			$this->calculateOrdering();
		}

        /**
        Option 4, calculate OF for each teaser and category
         */
        if ($this->input->getOption('teaserof'))
        {
            // step 0 clear redis data array
            $this->redis_data = [];


            // step0 - calc multiSA
            $this->calculateMultiSA();


            // step 1 - teaser OF logic
            $this->calculateFormula("preloadDataTeaser"
                , "evalFormulaTeaser"
                ,"Calculate Ordering Factor for each Teaser in each category (--teaserof)"
                ,\OrderingFactorHelper::REDIS_SUBKEY_TEASER
            );


            // step 2 - new articles OF logic
            $this->calculateFormula("preloadDataNewArticles"
                ,"evalFormulaNewArticles"
                ,"Calculate Ordering Factor for new articles by formula (--formula)"
                , \OrderingFactorHelper::REDIS_SUBKEY_NEWARTICLES
            );


            // step 3 redisDb update
            $this->saveFormulaToRedis();

            // step 4 clear all OF related cache
            $this->clearOFRelatedCache();

            //clear memory
            $this->redis_data = [];
        }

		/**
		Option 5, calculate new articles by flexible formula
		*/
		if ($this->input->getOption('formula'))
		{
            echo "--formula key is deprecated. Use --teaserof to calculate full logic.";
		}

		$ft = time();
		echo "===".PHP_EOL;
		echo "Total time of script execution: ".($ft-$st)." sec.".PHP_EOL;
        return 0;
    }

    /**
     * provider of calculated data in run time
     *
     * @param int $sa_id
     * @return array
     */
    public function getDataByKey(int $key) {
         return $this->redis_data[$key];
    } // getCalcDataBySaId

    /**
     * load --id-sa option into class property $sa_ids
     * @return void
     */
    private function loadSaIds() {
        $dbr = DB::getInstanceReconnectible(DB::USAGE_READ);
        if ($optionIdSa = $this->input->getOption('id-sa'))
        {
            $ids = [];
            foreach ($optionIdSa as $item) {
                $ids = array_merge($ids, explode(",", $dbr->escape($item)));
            }
            $this->sa_ids = array_unique($ids);
            echo "Calculation for SAs: ".implode(",",  $this->sa_ids).PHP_EOL;
        } else {
            $this->sa_ids = [];
        }
    }

    /**
     * clear all OF releted cache
     * @return void
     */
    private function clearOFRelatedCache() {
        /**
         * prepareSmartyParam
         * clear redis keys like this "~~1~~prepareSmartyParams(1,0)~~english~~"
         */
        \Shop::clearSmartyParamsCache();

        /**
         * getTopSellers
         * clear redis keys like this "~~12~~getTopSellers()~~polish~~"
         */
        cacheClear("getTopSellers%");

    }

    /**
     * update redis hash key \OrderingFactorHelper::REDIS_KEY_FORMULA
     * data from "saved_pic_ordering_factor"
     *
     *  Hash key in the redis DB is `saved_id`.
     *
     * @return void
     * @throws \Exception
     */
    private function saveFormulaToRedis() {

        $redis = RedisProvider::getInstance(RedisProvider::USAGE_CACHE);

        // two ways of redis update:
        // *********************************
        $redisKeyCount = 0;
        $success = true;
        if ( $this->sa_ids ) {
            foreach ($this->redis_data as $redis_key => $data_item) {
                $redisKeyCount++;
                $value = json_encode($data_item);
                $redisResult_set = $redis->hSet(\OrderingFactorHelper::REDIS_KEY_FORMULA, $redis_key, $value);
                if ($redisResult_set) {
                    echo "ERROR saving redis data, redis_key=".$redis_key.PHP_EOL;
                    $success = false;
                }
            }
        } else {
            $redis_data_hMset = [];
            foreach ($this->redis_data as $redis_key => $data_item) {
                $redis_data_hMset[$redis_key] = json_encode($data_item);
                $redisKeyCount++;
            } // foreach

            /**
             * hMset on prod with real data taken about 4 seconds,
             * to optimize this we need save and than rename
             */
            $SLEEP_TIME = 5; // 5 seconds
            $tempKeyName = "TEMP_".\OrderingFactorHelper::REDIS_KEY_FORMULA;

            $redis->del($tempKeyName); // just in case to exclude the situation when this key exist and has data
            sleep($SLEEP_TIME);

            $redisResult_hMset = $redis->hMset($tempKeyName, $redis_data_hMset);
            sleep($SLEEP_TIME);

            if ($redisResult_hMset === true) {
                $redisResult_rename = $redis->rename($tempKeyName, \OrderingFactorHelper::REDIS_KEY_FORMULA);
            } else {
                $redisResult_rename = false;
            }
            if ($redisResult_hMset !== true || $redisResult_rename !== true) {
                echo "ERROR saving redis data".PHP_EOL;
                $success = false;
            }

            unset($redis_data_hMset);
        }
        echo "===".PHP_EOL."Step: Redis update ".($success?"SUCCESS":"ERROR").". Count data keys (sa_id):".$redisKeyCount.PHP_EOL;
    } // saveFormulaToRedis

    /**
     * calculate multiSA groups and save them into $this->redis_data
     * by key \OrderingFactorHelper::REDIS_SUBKEY_MULTISA
     * @return false|void
     * @throws \Exception
     */
    private function calculateMultiSA() {
        $st = time();
        echo "===".PHP_EOL."calculate MultiSA".PHP_EOL;

        $dbr = DB::getInstanceReconnectible(DB::USAGE_READ);
        /**
         * optimized by run time query to calc multiSA
         * part 1 (before UNION ALL) calculates non master_sa SA's
         * part 2 (after UNION ALL) calculates only master_sa SA's
         */
        $sql = "
            SELECT sa.id sa_id
                   , GROUP_CONCAT(slave_sa.id) multi_sa
            FROM saved_auctions sa
            JOIN saved_auctions slave_sa ON 
                slave_sa.master_multi = sa.master_multi
                AND slave_sa.username = sa.username
                AND slave_sa.siteid = sa.siteid
                AND slave_sa.old = 0
                AND slave_sa.inactive = 0
                AND slave_sa.id <> sa.id
            WHERE 1
            ". (!empty($this->sa_ids) ? " AND sa.id in (".implode(",",  $this->sa_ids).")" : "" ) ."
            GROUP BY sa.id     
            
            UNION ALL
            
            SELECT sa.id sa_id
                    , GROUP_CONCAT(slave_sa.id) multi_sa
            FROM saved_auctions sa
            JOIN saved_auctions master_sa ON master_sa.id = sa.master_sa
            JOIN saved_auctions multi_sa ON multi_sa.master_multi = master_sa.master_multi
            JOIN saved_auctions slave_sa ON 
                slave_sa.master_sa = multi_sa.id
                AND slave_sa.username = sa.username
                AND slave_sa.siteid = sa.siteid
                AND slave_sa.old = 0
                AND slave_sa.inactive = 0
                AND slave_sa.id <> sa.id 
            WHERE 1
            ". (!empty($this->sa_ids) ? " AND sa.id in (".implode(",",  $this->sa_ids).")" : "" ) ."
            GROUP BY sa.id
        ";
        $sa_data = $dbr->getAssoc($sql);
        if (!$this->checkPearError( $sa_data )) return false;
        foreach($sa_data as $sa_id => $multi_sa) {
            $this->redis_data[$sa_id][\OrderingFactorHelper::REDIS_SUBKEY_MULTISA]=explode(",", $multi_sa);
        }

        $ft = time();
        echo "Count of found multiSA groups: ".count($sa_data).PHP_EOL;
        echo "--- finish calculateMultiSA (".($ft-$st)." sec.)".PHP_EOL;
    }

    /**
     ******************************************************************
     * --formula --teaserof
     * block of calculating OrderingFactor(OF) for new articles by flexible formula
     * and OF for each teaser in each category
     * @param string $preloader preloader data method name of  OrderingFactorVariables class
     * @param string $evaluator eval data method name of OrderingFactorVariables class
     * @param string $msg info message for log only
     * @param array $excludeSA it will be skipped all sa with id from this array
     * @throws \Exception
     * @version 2023-08-23
     * @author Andrey Shtokalo
     ******************************************************************
     */
	private function calculateFormula($preloader, $evaluator, $msg, $redis_dataKey)
	{

		echo "===".PHP_EOL;
        echo $msg.PHP_EOL;

        /********************************************************
        * STEP 0, preload ordering data for formula calculate
        ********************************************************/
        $OFVariables = new \OrderingFactorVariables();
        if ($OFVariables->{$preloader}($this->sa_ids) === false) {
            echo "calculateFormula stopped".PHP_EOL;
            return;
        };

        $generatedSA = [];
        $used = []; // to get saved in DB and not generated values to set them OF=0
        $cheatFormula = $this->input->getOption('cheatFormula');

        foreach($OFVariables->{$evaluator}( $cheatFormula, $this ) as $item)
        {
            $count_generated++;
            $index = $item["sa_id"]."_".$item["cat_id"]."_".$item["doc_id"]."_".$item["doc_type"];
            $used[$index] = 1;

            if ($item["ordering_factor"]==-1) continue;

            $this->redis_data[$item['sa_id']][$redis_dataKey][] = [
                "di"=>$item['doc_id'],
                "dt"=>$item['doc_type'],
                "ci"=> $item['cat_id'],
                // string value at once to more compact in json structure because float value has many decimal places
                "of" => (string)round((float)$item["ordering_factor"], \OrderingFactorVariables::OF_PRECISION)
            ];

        } // generator evalFormula()

        echo "  Ordering factor generated values: ".$count_generated.". Unique SA count in work array: ".count($this->redis_data).PHP_EOL;
        echo "--- finish calculateFormula command".PHP_EOL;
    } //calculateFormula

    /**
     * save CTR data from redis into mySQL
     */

    /**
	******************************************************************
		--savefromredis
		block of REDIS CTR data processing 
		@author Andrey Shtokalo
	******************************************************************
	*/
    private function processSaveFromRedis()
    {
        $redis = RedisProvider::getInstance(RedisProvider::USAGE_CACHE);
        $currentTime = time();

        echo "===".PHP_EOL;
        echo "Move CTR data from Redis DB to mySQL (--savefromredis)".PHP_EOL;
        echo "===".PHP_EOL;

        $session_live_time = \OrderingFactorHelper::getCTRSessionLiveTime();
        if ( $maxTime_str = $this->input->getOption('maxdate') ) {
            $maxTime = strtotime( $maxTime_str );
        } else {
            // minus $session_live_time(2 hours) because it needs to stay data on $session_live_time hours ago
            $maxTime = strtotime ( "-{$session_live_time}hours" ,  $currentTime );
        }
        $MAX_OVER_HOURS = 5; // max possible hours to over current time
        echo "maxTime=" . date("Y-m-d H:i:s", $maxTime) . "({$maxTime}), currentTime="
                         .date("Y-m-d H:i:s", $currentTime) ."({$currentTime})" . PHP_EOL;

        /********************************************************
         * STEP 1, prepare data
         ********************************************************/

        $sql_data = [];
        $sql_data_session = [];
        $redis_data = [];
        $wrong_time = [];
        $current_redis_data = $redis->hGetAll(\OrderingFactorHelper::REDIS_KEY);
        $minTime = $maxTime;

        foreach ($current_redis_data as $redis_key => $data_str) {
            $data = json_decode($data_str, true);

            // find saved_id in the $redis_key
            $key_arr = explode('_', $redis_key);
            if (count($key_arr) != 3) {
                echo 'WARNING processSaveFromRedis: wrong key:' . $redis_key . "'" . PHP_EOL;
                continue;
            }
            $saved_id = (int)$key_arr[2];

            foreach ($data as $session => $ctr_data) {

                foreach ($ctr_data as $ind => $time_arr) {
                    foreach ($time_arr as $time) {
                        $dateKey = date("Y-m-d", $time);
                        $sql_data_key = $redis_key . "_" . $dateKey;
                        $sql_data_session_key = $saved_id.  "_" . $dateKey;
                        if ($time <= $maxTime) {

                            // to save in mySQL
                            if (!isset($sql_data[$sql_data_key])) $sql_data[$sql_data_key] = [];
                            if (!isset($sql_data[$sql_data_key][$ind])) $sql_data[$sql_data_key][$ind] = 0;
                            $sql_data[$sql_data_key][$ind]++;

                            // session
                            if (!isset($sql_data[$sql_data_key]['session'])) {
                                $sql_data[$sql_data_key]['session'] = [];
                            }
                            if (!in_array($session, $sql_data[$sql_data_key]['session'])) {
                                $sql_data[$sql_data_key]['session'][] = $session;
                            }

                            // for count unique sessions for the shop
                            if (!isset($sql_data_session[$sql_data_session_key])) {
                                $sql_data_session[$sql_data_session_key] = [];
                            }
                            if (!in_array($session, $sql_data_session[$sql_data_session_key])) {
                                $sql_data_session[$sql_data_session_key][] = $session;
                            }

                            // time
                            if ($time < $minTime) $minTime = $time;

                        } else {
                            if ($time > ($currentTime + 60 * 60 * $MAX_OVER_HOURS)) { // incorrect $time
                                if (!isset($wrong_time[$dateKey])) $wrong_time[$dateKey] = 0;
                                $wrong_time[$dateKey]++;
                            } else {

                                // stay in redisDB
                                if (!isset($redis_data[$redis_key])) $redis_data[$redis_key] = [];
                                if (!isset($redis_data[$redis_key][$session])) $redis_data[$redis_key][$session] = [];
                                if (!isset($redis_data[$redis_key][$session][$ind])) $redis_data[$redis_key][$session][$ind] = [];

                                $redis_data[$redis_key][$session][$ind][] = $time;
                            }
                        }
                    } // foreach time_arr by timestamps
                } // foreach ctr_data by impressions/clicks

            } // foreach data by sessions
        } // foreach current_redis_data

        // $wrong_time report
        if ( !empty($wrong_time ) ) {
            $dateStr = date("Y-m-d H:i:s", $currentTime+60*60*$MAX_OVER_HOURS);
            echo "WARNING. Data with these timestamps were skipped because this is a future time (more than {$dateStr} utc) :  <pre>".print_r($wrong_time, true)."</pre>";
        }
        echo "Prepared: save to mySQL '".count($sql_data)."' keys(teasers) ".PHP_EOL;
        echo "Prepared: stay to RedisDB '".count($redis_data)."' keys(teasers) ".PHP_EOL;
        echo "---".PHP_EOL;
        /********************************************************
        STEP 2, save data into mySQL and Redis
         ********************************************************/

        if ( count($sql_data) ) {

            //save sessions for shop
            if ( count($sql_data_session) ) {
                if ( !$this->processSessionToSQL($sql_data_session, $minTime, $maxTime) ) {
                    echo "ERROR: Did not save CTR SESSION data into mySQL DB".PHP_EOL;
                }
            } // sql_data_session

            if ( $this->processCTRToSQL($sql_data, $minTime, $maxTime) ) {

                // clear redis ctr data
                $redis->del(\OrderingFactorHelper::REDIS_KEY);

                // save new redis data array
                if ( count($redis_data) ) {
                    // prepare json data
                    $redis_data_toSetAll = [];
                    foreach ($redis_data as $redis_key => $data_item) {
                        $redis_data_toSetAll[$redis_key] = json_encode($data_item);
                    }
                    // save data into the REDIS

                    // set CTR data
                    $redisSaveResult = $redis->hMset(\OrderingFactorHelper::REDIS_KEY, $redis_data_toSetAll);
                    $redis->expire(\OrderingFactorHelper::REDIS_KEY, \OrderingFactorHelper::DEFAULT_TTL);

                    if ($redisSaveResult) {
                        echo "CTR Redis data was reset, new key's count: ".count($redis_data_toSetAll)
                            ."; before key's count: ".count($current_redis_data).PHP_EOL;
                    } else {
                        echo "ERROR: wtite CTR data into redis db error of ".count($redis_data_toSetAll)." items.".PHP_EOL;
                        \ServerLogs::createLogFile( static::LOG_NAME, "redis_error", "wtite data error of "
                            .count($redis_data_toSetAll)." items.");
                    }

                } else {
                    echo "CTR Redis data was cleared".PHP_EOL;
                }

            } else {
                echo "ERROR: Did not save CTR data into mySQL DB".PHP_EOL;
            }
        } // if mySQL

        echo "--- finish --savefromredis command".PHP_EOL;

    } // process

    /**
     * save CTR session data into shop_ctr_session_collect
     * using bath saving
     * @param array $sql_data - ctr session data from redis
     * @param int $minTime - min time of CTR data to reduce SQL selection
     * @param int $maxTime - max time of CTR data  to reduce SQL selection
     * @return bool true/false - success/fail
     * @throws \Exception
     */
    private function processSessionToSQL(array $sql_data, int $minTime, int $maxTime)
	{
		$dbr = DB::getInstanceReconnectible(DB::USAGE_READ);
        $min_date = date("Y-m-d", $minTime);
        $max_date = date("Y-m-d", $maxTime);

        // load shop_id for collected saved_id
        $sa_ids = array_unique(array_map(fn($v) => explode("_", $v)[0], array_keys($sql_data) ));
		$sql = "SELECT sa.id k, shop.id 
		          FROM saved_auctions sa
				  JOIN shop ON shop.username=sa.username AND shop.siteid=sa.siteid
				  WHERE sa.id in (".implode(",", $sa_ids).")";
		$shops = $dbr->getAssoc($sql);
		if (!$this->checkPearError( $shops )) return false;

        // calc unique sessions array for each shop
		$sessions_byShop = [];
		foreach($sql_data as $sql_data_key => $sessions_for_saved_id) {
            [$saved_id, $date_key] = explode("_", $sql_data_key);
			foreach($sessions_for_saved_id as $session) {
				if ($shops[ $saved_id ]) {
                    $sessions_byShop_key = $shops[ $saved_id ] . "_" . $date_key;
                    if (!isset($sessions_byShop[ $sessions_byShop_key ])) $sessions_byShop[ $sessions_byShop_key ] = [];
					$sessions_byShop[ $sessions_byShop_key ][ $session ] = 1;
				}
			}
		}

		// load current data of shop_ctr_session_collect
		$sql = 'SELECT CONCAT(shop_id,"_",date) k
		        , session
                , id 
                FROM shop_ctr_session_collect 
                WHERE date <= DATE("'.$max_date.'") AND date >= DATE("'.$min_date.'")';
		$current_data = $dbr->getAssoc($sql);
		if (!$this->checkPearError( $current_data )) return false;
		
		// prepare arrays for insert or update data
		$update_arr = [];
		$insert_arr = [];
		foreach ($sessions_byShop as $sessions_byShop_key => $sessions_array) {
			$session = count($sessions_array);
            [$shop_id, $date_key] = explode("_", $sessions_byShop_key);
			if ( isset( $current_data[$sessions_byShop_key] ) ) {
				$current_item = $current_data[$sessions_byShop_key];
				$update_arr[$current_item['id']] = "WHEN id = {$current_item['id']} THEN "
				                                   .($session+(int)$current_item['session'])." ";
			} else {
				$insert_arr[] = "(DATE(\"{$date_key}\"),{$shop_id},{$session})";
			}
		}
		echo "Insert count into `shop_ctr_session_collect`: ".count($insert_arr).PHP_EOL;
		echo "Update count into `shop_ctr_session_collect`: ".count($update_arr).PHP_EOL;
		
	    $insert_query = "
          INSERT INTO shop_ctr_session_collect  
          (date, shop_id, session )
          VALUES @VALUES";
		if ( !empty( $insert_arr ) )  {
			SQLHelper::runQueryByChunkValues($insert_query, $insert_arr, ",", 3000, 3, true);
		}
		
		$update_query = "
		  UPDATE shop_ctr_session_collect  
		  SET session = CASE
			@VALUES
			ELSE session
		  END 
		  WHERE ID in (@IDS_VALUES)";
		if ( !empty( $update_arr ) )  {
			SQLHelper::runQueryByChunkValues($update_query, $update_arr, " ", 3000, 3, true);
		}
		
		echo "--- finish OrderingFactorCommand.processSessionToSQL".PHP_EOL;
		return true;
	} // processSessionToSQL

    /**
     * save impression and click (ctr) data into saved_pic_ctr_collect
     * using bath saving
     * @param array $sql_data - ctr data from redis
     * @param int $minTime - min time of CTR data to reduce SQL selection
     * @param int $maxTime - max time of CTR data  to reduce SQL selection
     * @return bool true/false - success/fail
     * @throws \Exception
     */
    private function processCTRToSQL(array $sql_data, int $minTime, int $maxTime)
    {
		$dbr = DB::getInstanceReconnectible(DB::USAGE_READ);
		$update_fields = ['click', 'impression', 'session'];
		$min_date = date("Y-m-d", $minTime);
        $max_date = date("Y-m-d", $maxTime);

        //current data
		$sql = 'SELECT CONCAT(doc_id,"_",doc_type,"_",saved_id,"_",date) as k
                , impression
                , click
                , session
                , id 
                FROM saved_pic_ctr_collect 
                WHERE date <= DATE("'.$max_date.'") AND date >= DATE("'.$min_date.'")';
		$current_data = $dbr->getAssoc($sql);
		if (!$this->checkPearError( $current_data )) return false;

        // check existing doc_id before insert or update
        // ************************************************
        $doc_ids = array_map(fn($v) => explode("_", $v)[0], array_keys($sql_data) );
        $doc_ids = array_unique($doc_ids);
        $check_query = "SELECT doc_id FROM saved_pic WHERE doc_id IN (".implode(",",$doc_ids).")";
        $check_doc_ids = $dbr->getCol($check_query);
        if (!$this->checkPearError( $check_doc_ids )) return false;
        // there are all deleted doc_id - we cannot save this doc_id
        $doc_ids_deleted = array_diff($doc_ids, $check_doc_ids);

		// iteration by $sql_data
		$update_arr = [];
		$insert_arr = [];
		foreach ($sql_data as $key => $data) {
            $key_arr = explode('_', $key);

            // $key should be like this structure: "765043_whitesh_523_2023-12-12"
            // check just in case
            if (count($key_arr) != 4) {
				echo 'WARNING processCTRToSQL: wrong key:'.$key."'".PHP_EOL;
                continue;
            }

            $doc_id = (int) $key_arr[0];
            $doc_type = $key_arr[1];
			$saved_id = (int) $key_arr[2];
            $date_key = $key_arr[3];
			$impression = (int)$data[\OrderingFactorHelper::IMPRESSION];
			$click = (int)$data[\OrderingFactorHelper::CLICK];
			$session = isset($data['session']) && is_array($data['session']) ? count($data['session']) : 0;

            // check doc_id if it has been deleted
			if (!empty($doc_ids_deleted) && in_array($doc_id, $doc_ids_deleted)) {
                echo 'WARNING processCTRToSQL: skipped key:"'.$key.'". Reason doc_id="'.$doc_id.'" has been deleted'.PHP_EOL;
                continue;
            }

			//check data for wrong values
			if ( $saved_id==0 || $doc_id==0 || !in_array($doc_type, \OrderingFactorHelper::DOC_TYPE_ENUM) ) 
			{
				echo "WARNING: Skipped row. Can not save this Redis data: saved_id={$saved_id}, doc_id={$doc_id }, doc_type={$doc_type},"
				      ." impression={$impression}, click={$click}".PHP_EOL;
				continue;		  
			}	
			
			if ( $current_data[$key] ) {
				$current_item = $current_data[$key];
				
				// update if it needs only
				foreach($update_fields as $ind ) {
					if (${$ind} > 0) {
						
						// sum redis value and current mySQL value
						$update_arr[$ind][$current_item['id']] = "WHEN id = {$current_item['id']} THEN ".
						    (int)(${$ind}+$current_item[$ind])." ";
							
					}
				}
				
			} else {
				$insert_arr[] = "(DATE(\"{$date_key}\"),{$saved_id},{$doc_id},\"{$doc_type}\",{$impression},{$click},{$session})";
			}
            
        } // foreach sql_data
		
		echo "Insert count into `saved_pic_ctr_collect`: ".count($insert_arr).PHP_EOL;

		$insert_query = "
          INSERT INTO saved_pic_ctr_collect  
          (date, saved_id, doc_id, doc_type, impression, click, session )
          VALUES @VALUES";
		if ( !empty( $insert_arr ) )  {
			SQLHelper::runQueryByChunkValues($insert_query, $insert_arr, ",", 3000, 3, true);
		}
		
		foreach($update_fields as $ind ) {
			echo "Update count of `${ind}` of `saved_pic_ctr_collect`: ".count($update_arr[$ind]).PHP_EOL;
			$update_query = "
			  UPDATE saved_pic_ctr_collect  
			  SET {$ind} = CASE
				@VALUES
				ELSE {$ind}
			  END 
			  WHERE ID in (@IDS_VALUES)";
			if ( !empty( $update_arr[$ind] ) )  {
				SQLHelper::runQueryByChunkValues($update_query, $update_arr[$ind], " ", 3000, 3, true);
			}
		} // foreach
		
		echo "--- finish OrderingFactorCommand.processCTRToSQL".PHP_EOL;
		return true;
	} // processCTRToSQL
	
	/**
	******************************************************************
		--ordering
		block of calculate ordering
		@author Andrey Shtokalo
	******************************************************************
	*/
	
	private function calculateOrdering()
	{
		echo "===".PHP_EOL;
		echo "Calculate Ordering (--ordering)".PHP_EOL;
		echo "===".PHP_EOL;
		
		// seen_on_screen field of saved_autions table
		echo "---".PHP_EOL;
		echo "Calculate Ordering 1: update seen_on_screen".PHP_EOL;
		echo "---".PHP_EOL;
		$this->updateSeenOnScreen();
		
		// sleep after update saved_auctions table
		if (APPLICATION_ENV === 'production') {
			sleep(60); 
		} else {
			// for more fast debug
			sleep(5); 
		}
		
		// ordering_factor field of saved_autions table
		echo "---".PHP_EOL;
		echo "Calculate Ordering 2: update ordering_factor".PHP_EOL;
		echo "---".PHP_EOL;
		$this->updateOrderingFactor();
		
		echo "--- fininsh --ordering command".PHP_EOL;
		
	} // calculateOrdering

	/**
     * recalculate seen_on_screen field in `saved_auctions` table
	 * using source data from `saved_pic_ctr_collect` table
	 * using shop.total_brutto_income_period setting
     */
    private function updateSeenOnScreen()
    {
		$db = \label\DB::getInstanceReconnectible(\label\DB::USAGE_WRITE);
        $dbr = \label\DB::getInstanceReconnectible(\label\DB::USAGE_READ);
		
		$q = "SELECT id, seen_on_screen FROM saved_auctions";
		$current_data = $dbr->getAssoc($q);
		$this->checkPearError($current_data);
		echo "Count of rows of `saved_auctions` table: ".count($current_data).PHP_EOL;
		
        $calc_data = $dbr->getAssoc("
            SELECT spcc.saved_id, SUM(spcc.impression) impression, SUM(spcc.click) click
            FROM saved_pic_ctr_collect spcc
            JOIN saved_auctions sa ON sa.id = spcc.saved_id
            JOIN shop ON shop.username = sa.username AND shop.siteid = sa.siteid

            WHERE spcc.date > DATE_ADD(CURRENT_TIMESTAMP, INTERVAL -shop.total_brutto_income_period DAY)

            GROUP BY spcc.saved_id");
		if (!$this->checkPearError($calc_data)) {
			echo "ERROR: updateSeenOnScreen stopped".PHP_EOL;
			return false;
		};
		echo "Count of data rows of `saved_pic_ctr_collect` table: ".count($calc_data).PHP_EOL;
		$update_arr = [];
		$count_theSame = 0; // counter for the same value for log
		// compare current and calc data
		foreach ($calc_data as $saved_id => $item) {
			
			// temp descision - calc impression as seen_of screen
			$time = (int)$item['impression'];
			
			if ( array_key_exists($saved_id, $current_data) ) {
				// uppdate only if there is changes
				if ( $current_data[$saved_id] != $time ) {
					$update_arr[$saved_id] = "WHEN id = {$saved_id} THEN {$time} ";
				} else {
					$count_theSame++;
				}
				// marker means that this saved_id is present in the calculation
				$current_data[$saved_id] = -1; 
				
			} else {
				// impossible situation saved_id is not in the saved_auction
				echo "WARNING unknown saved_id={$saved_id} in the saved_pic_ctr_collect";
			}		

		} // $calc_data
		
		// set zerro for out of calculate SA
		$count_zero = 0; // counter of zerro update for log 
		$count_theSame_zero = 0; // counter of the same values = zero 
        foreach ($current_data as $saved_id => $time) {
			// if saved_id is out of the calculation then data will be set to zerro
			if ($time > 0) {
				$update_arr[$saved_id] = "WHEN id = {$saved_id} THEN 0";
				$count_zero++;
			}
			
			if ($time == 0) {
				$count_theSame_zero++;
				$count_theSame++;
			}
        } // foreach current_data
		echo "---".PHP_EOL;
		echo "saved_auctions seen_on_screen update total count: ".count($update_arr).", including set to zero: ".$count_zero.PHP_EOL;
		echo "saved_auctions seen_on_screen the same (not updated) total count values: ".$count_theSame
		                                  .", including non-zero values: ".($count_theSame-$count_theSame_zero).PHP_EOL;
		$update_query = "
          UPDATE saved_auctions  
          SET seen_on_screen = CASE
            @VALUES
		    ELSE seen_on_screen
		  END 
		  WHERE ID in (@IDS_VALUES)";
		if ( !empty( $update_arr ) )  {
			SQLHelper::runQueryByChunkValues($update_query, $update_arr, " ", 3000, 3, true);
		}
    } // updateSeenOnScreen
	
    /**
     * update ordering_factor field in saved_auctions table
     */
    private function updateOrderingFactor()
    {
		$db = \label\DB::getInstanceReconnectible(\label\DB::USAGE_WRITE);
		$dbr = \label\DB::getInstanceReconnectible(\label\DB::USAGE_READ);
		
		// structure: [[shop_id] => [source=> [10,12]]];
        $total_brutto_income_source = $this->getBruttoSourcesForOrdering();

	
		$update_arr = []; 
		// filling update_arr by shopls	
        foreach ($total_brutto_income_source as $shop_id => $item) {

            $shop_id = (int) $shop_id;
            $ordering_factors = $this->getOrderingFactor($shop_id, $item['source']);
			
			echo "shop_id : $shop_id, update count: ".count($ordering_factors).PHP_EOL;
			if (!empty($ordering_factors)) echo print_r($ordering_factors, true).PHP_EOL;
			
            foreach ($ordering_factors as $id => $factor) {
                $id = (int) $id;
                $factor = (float) $factor;
				$update_arr[$id] = "WHEN id = {$id} THEN {$factor} ";
            }
        }
		
		// execute update queries
		if (!empty($update_arr)) {
			$update_query = "
			  UPDATE saved_auctions  
			  SET ordering_factor = CASE
				@VALUES
				ELSE ordering_factor
			  END 
			  WHERE ID in (@IDS_VALUES)";
			SQLHelper::runQueryByChunkValues($update_query, $update_arr, " ", 3000, 3, true);
		}
		echo "---".PHP_EOL;
		echo "Total update count: ".count($update_arr).PHP_EOL;
    } // updateOrderingFactor
	
    /**
     * Вытаскиваем все параметры для каждого шопа, для расчета брутто
     *
     * @return type
	 * @author AlexS_10167, refactored
     */
    private function getBruttoSourcesForOrdering()
    {
        $dbr = \label\DB::getInstanceReconnectible(\label\DB::USAGE_READ);

        $total_brutto_income_source = $dbr->getAssoc("SELECT id, ordering_recalculation_freq, 
                    CONCAT(id, '.', IFNULL(total_brutto_income_source, '')) source
                FROM shop WHERE NOT inactive");

        if( !$this->checkPearError($total_brutto_income_source) ) {
			echo "getBruttoSourcesForOrdering stopped".PHP_EOL;
			return false;
        }

        array_walk($total_brutto_income_source, function (&$item) {
            $item['source'] = explode('.', $item['source']);
            $item['source'] = array_map('intval', $item['source']);
            $item['source'] = array_values(array_unique(array_filter($item['source'])));
            return $item;
        });

        return $total_brutto_income_source;
    } // getBruttoSourcesForOrdering
	
      /**
     * Получаем OF для шопа
     *
     * @param type $shop_id
     * @param type $source
     * @return type
	 * @author AlexS_10167
     */
    private function getOrderingFactor($shop_id, $source)
    {
        $dbr = \label\DB::getInstanceReconnectible(\label\DB::USAGE_READ);

        if (!count($source) || count($source) == 1) {
            $query = $this->getOrderingFactorSingleQuery($shop_id, $source);
        } else {
            $query = $this->getOrderingFactorQuery($shop_id, $source);
        }
		
		// only difference values of ordering factor
		$query = "SELECT id, new_ordering_factor 
		 FROM ( {$query}
		) qwe WHERE ROUND(new_ordering_factor*100) != ROUND(ordering_factor*100)";
		
        //if ($shop_id == 12) echo $query . "\n"; // @debug
        $ordering_factors = $dbr->getAssoc($query);
		if (!$this->checkPearError($ordering_factors)) {
			echo "ERROR run query for shop ".$shop_id.PHP_EOL;
			return [];
		};
		
		$ordering_factors = (array) $ordering_factors;
        $ordering_factors = array_map('floatval', $ordering_factors);
        return $ordering_factors;
    }

    /**
     * Если указан только 1 источник брутто, получаем его
     *
     * @param type $shop_id
     * @param type $source
     * @return type
	 * @author AlexS_10167
     */
    private function getOrderingFactorSingleQuery($shop_id, $source)
    {
        return "SELECT sa.id, sa.ordering_factor, 
		           IFNULL((IF (IFNULL(sa.timed_brutto_income, 0) > 0, sa.timed_brutto_income, 0) / sa.seen_on_screen), 0) new_ordering_factor
                FROM saved_auctions sa
                JOIN shop ON shop.username = sa.username
                    AND shop.siteid = sa.siteid
                WHERE shop.id = '$shop_id' AND NOT shop.inactive";
    }

    /**
     * Еслиуказано несколько источников брутто, расчитываем их
     *
     * @param type $shop_id
     * @param type $source
     * @return type
	 * @author AlexS_10167, refactored
     */
    private function getOrderingFactorQuery($shop_id, $source)
    {
		/** 
		* query optimize:
		* construction IFNULL(sa_inside.master_sa, sa_inside.id) = IFNULL(sa.master_sa, sa.id) takes too long time
		* desision: using different logic for shop=1 and others because there are master_sa only on shop=1
		*/
		if ($shop_id == 1) {
			$sa_condition = "sa.id";
		} else {
			$sa_condition = "sa.master_sa";
		}
		if ( in_array(1, $source) ) {
			$sa_inside_condition = "IFNULL(sa_inside.master_sa, sa_inside.id)";
		} else {
			$sa_inside_condition = "sa_inside.master_sa";
		}
		
        return "SELECT id, ordering_factor, 
		    IFNULL((IF (IFNULL(timed_brutto_income, 0) > 0, timed_brutto_income, 0) / seen_on_screen), 0) new_ordering_factor
            FROM (
                SELECT sa.id, sa.ordering_factor,
                (SELECT SUM(timed_brutto_income) FROM saved_auctions sa_inside 
                    JOIN shop shop_inside ON shop_inside.username = sa_inside.username
                        AND shop_inside.siteid = sa_inside.siteid
                    WHERE {$sa_condition} = {$sa_inside_condition}
                        AND shop_inside.id IN (" . implode(',', $source) . ") 
                        AND NOT shop_inside.inactive) timed_brutto_income, 

                (SELECT SUM(seen_on_screen) FROM saved_auctions sa_inside 
                    JOIN shop shop_inside ON shop_inside.username = sa_inside.username
                        AND shop_inside.siteid = sa_inside.siteid
                    WHERE {$sa_condition} = {$sa_inside_condition}
                        AND shop_inside.id IN (" . implode(',', $source) . ") 
                        AND NOT shop_inside.inactive) seen_on_screen

                FROM saved_auctions sa
                JOIN shop ON shop.username = sa.username
                    AND shop.siteid = sa.siteid
                WHERE shop.id = '$shop_id' AND NOT shop.inactive
            ) t";
    }

	/**
	******************************************************************
		--brutto
		block of brutto calculate functions
		@author AlexS_10167
		@todo refactoring this block
	******************************************************************
	*/
	
    /**
     * Расчитываем брутто для всех СА
     * Расчет брутто происходит за время указаное в настройке $shop->total_brutto_income_period
     */
    private function calculateBrutto()
    {
        $db = \label\DB::getInstanceReconnectible(\label\DB::USAGE_WRITE);
        $dbr = \label\DB::getInstanceReconnectible(\label\DB::USAGE_READ);

		echo "===".PHP_EOL;
		echo "Calculate Brutto (--brutto)".PHP_EOL;
		echo "===".PHP_EOL;

        /**
         * Получаем максимальные $shop->total_brutto_income_period
         */
        $time = $dbr->getOne("SELECT MAX(total_brutto_income_period) FROM shop WHERE NOT inactive");
		if (!$this->checkPearError($time)) {
			echo "ERROR: stoped calculateBrutto".PHP_EOL;
			return;
		}
		$time = (int) $time;
        $time = date('Y-m-d 00:00:00', strtotime("-$time days"));

        $auctions = $this->getAuctions($time);
        $this->processBrutto($auctions);
		
		echo "--- finish --brutto command".PHP_EOL;
    }

    /**
     * Пересчитываем брутто
     *
     * @param type $auctions
     */
    private function processBrutto($auctions)
    {
        echo "NOT save mode!".PHP_EOL;
        $this->loadSaIds();

        $db = \label\DB::getInstanceReconnectible(\label\DB::USAGE_WRITE);
        $dbr = \label\DB::getInstanceReconnectible(\label\DB::USAGE_READ);

        $processed_ids = [];
        $queryForTotalIncomePeriod = "SELECT id, total_brutto_income_period FROM shop WHERE NOT inactive";

        $shops = $dbr->getAssoc($queryForTotalIncomePeriod);
        if (!$this->checkPearError($shops)) return null;

		$count_upd = 0;
        foreach ($shops as $shop_id => $total_brutto_income_period) {
            $time = (int) $total_brutto_income_period;
            $time = date('Y-m-d 00:00:00', strtotime("-$time days"));

            if (!isset($auctions[$shop_id])) {
                continue;
            }

            foreach ($auctions[$shop_id] as $saved_id) {

                if ( !empty($this->saved_ids) && !in_array($saved_id, $this->saved_ids) ) continue;

                ["brutto" => $brutto, "purchases" => $purchases] = array_map("floatval",
                    get_object_vars(
                        $this->getBruttoForSA($time, $saved_id)
                    )
                );
                /*
                 * update depricated - for log only
                $queryResult = $db->query("UPDATE saved_auctions
                      SET timed_brutto_income = '$brutto' 
                          , timed_purchases = '$purchases'
                      WHERE id = '$saved_id'");
                */
				$count_upd++;
				$this->checkPearError($queryResult);
                $processed_ids[] = $saved_id;

                echo "UPDATE saved_auctions SET timed_brutto_income = '$brutto', timed_purchases = '$purchases' WHERE id = '$saved_id'\n";
            }
        }

        if (empty($this->sa_ids)) { // mass update
            if ($processed_ids) {
                /*
                 * update depricated - for log only
                $queryResult = $db->query("UPDATE saved_auctions
                                       SET timed_brutto_income = '0.0',  timed_purchases = '0.0'
                                       WHERE id NOT IN (" . implode(',', $processed_ids) . ")");
                */
                $count_upd++;
                $this->checkPearError($queryResult);
                echo "UPDATE saved_auctions SET timed_brutto_income = '0.0', timed_purchases = '0.0' WHERE id NOT IN (" . implode(',', $processed_ids) . ")\n";
            } else {
                /*
                 * update depricated - for log only
                $queryResult = $db->query("UPDATE saved_auctions SET timed_brutto_income = '0.0', timed_purchases = '0.0'");
                */
                $count_upd++;
                $this->checkPearError($queryResult);
                echo "UPDATE saved_auctions SET timed_brutto_income = '0.0', timed_purchases = '0.0'\n";
            }
        }
		echo "Total count of UPDATE (not updated! - just log mode): {$count_upd}".PHP_EOL;
		return true;
    }

    /**
     * Получаем брутто для СА за интервал времени $time
     *
     * @param type $time
     * @param type $saved_id
     * @return type
     */
    private function getBruttoForSA($time, $saved_id)
    {
        $dbr = \label\DB::getInstanceReconnectible(\label\DB::USAGE_READ);

        $q = "SELECT GROUP_CONCAT(CONCAT(IFNULL(mau.auction_number, 0), ',', auction.auction_number))
            FROM orders
            JOIN article ON orders.article_id=article.article_id AND article.admin_id=orders.manual
            JOIN auction ON auction.auction_number=orders.auction_number AND auction.txnid=orders.txnid
            LEFT JOIN auction mau ON auction.main_auction_number=mau.auction_number AND auction.main_txnid=mau.txnid
            JOIN invoice ON invoice.invoice_number = auction.invoice_number

            WHERE NOT orders.manual 
                and auction.saved_id = $saved_id
                AND invoice.invoice_date > '$time' 
                AND invoice.invoice_date <= NOW()";

        $auctions = $dbr->getOne($q);
//                AND date(auction.delivery_date_real) > '$time'
//                AND date(auction.delivery_date_real) <= NOW()");
		$this->checkPearError($auctions);
        $auctions = explode(',', $auctions);
        $auctions = array_map('intval', $auctions);

        $result = (object)["brutto"=>0,"purchases"=>0];
        if ($auctions) {
            $q = "SELECT SUM(IFNULL(ac.brutto_per_article, 0)) brutto
                       , SUM(IFNULL(al.default_quantity, 0)) purchases
                FROM auction_calcs ac
                JOIN article_list al ON ac.article_list_id = al.article_list_id AND al.group_id
                JOIN article a ON al.article_id = a.article_id AND NOT a.admin_id
                WHERE ac.auction_number IN (" . implode(',', $auctions) . ")";
            $result = $dbr->getRow($q);
			$this->checkPearError($result);
        }
        return $result;
    }

    /**
     * Вытаскиваем все аукционы за период $time
     *
     * @param type $time
     * @return array
     */
    private function getAuctions($time)
    {
        $dbr = \label\DB::getInstanceReconnectible(\label\DB::USAGE_READ);

        $start = microtime(true);

        $query = "SELECT shop.id shop_id
                , GROUP_CONCAT(distinct auction.saved_id order by auction.saved_id) auctions

                FROM orders
                JOIN article ON orders.article_id=article.article_id AND article.admin_id=orders.manual
                JOIN auction ON auction.auction_number=orders.auction_number AND auction.txnid=orders.txnid
                LEFT JOIN auction mau ON auction.main_auction_number=mau.auction_number AND auction.main_txnid=mau.txnid
                JOIN invoice ON invoice.invoice_number = auction.invoice_number

                JOIN saved_auctions sa ON sa.id = auction.saved_id
                JOIN shop ON shop.username = sa.username AND shop.siteid = sa.siteid AND NOT shop.inactive
            WHERE NOT orders.manual 
                AND invoice.invoice_date > '$time' 
                AND invoice.invoice_date <= NOW()
            ". ( !empty($this->sa_ids) ? " AND sa.id in (".implode(",",  $this->sa_ids).")" : "" ). "
            GROUP BY shop_id ";
//                AND date(auction.delivery_date_real) > '$time'
//                AND date(auction.delivery_date_real) <= NOW()";

        $auctionsFromBD = $dbr->getAssoc($query);
		$this->checkPearError($auctionsFromBD);
		
        foreach ($auctionsFromBD as $shopId => $auctions) {
            $auctionsInArray = explode(',', $auctions);
            $auctionsFromBD[$shopId] = $auctionsInArray;
        }

        return $auctionsFromBD;
    }

} // OrderingFactorCommand
