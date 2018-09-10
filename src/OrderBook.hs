{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE FlexibleContexts #-}
module OrderBook where

import Data.Hashable (Hashable)
import qualified Data.HashMap as HM
import qualified Data.Heap as H
import System.Random
import Control.Concurrent.MVar
import Control.Concurrent
import Data.Maybe
import Control.Monad
import System.Clock

newtype OrderId = OrderId Integer
  deriving (Eq, Ord, Read, Show, Hashable)

data OrderType = Ask | Bid
  deriving (Eq, Ord, Read, Show)

data Order = Order {
  ordId :: OrderId,
  ordPrice :: Integer,
  ordAmount :: Integer,
  ordType :: OrderType
}
  deriving (Eq, Ord, Read, Show)

data Limit = Limit {
  lmtPrice :: Integer,
  lmtOrders :: [OrderId]
}
  deriving (Eq, Ord, Read, Show)

data OrderBookStats = OrderBookStats {
  stOrderCount :: !Integer,
  stTradeCount :: !Integer,
  stCreated :: !TimeSpec
 }
  deriving (Eq, Ord, Read, Show)

emptyStats created = OrderBookStats 0 0 created

incrementOrderCount stats = stats { stOrderCount = (stOrderCount stats) + 1}
incrementTradeCount stats = stats { stTradeCount = (stTradeCount stats) + 1}

data OrderBook = OrderBook {
  ordBookOrderMap :: HM.Map OrderId Order,
  ordBookAskLimitMap :: HM.Map Integer Limit,
  ordBookBidLimitMap :: HM.Map Integer Limit,
  ordBookAskLimits :: H.MinHeap Integer,
  ordBookBidLimits :: H.MaxHeap Integer,
  ordBookStats :: OrderBookStats
}
  deriving (Eq, Ord, Read, Show)

addOrder :: OrderBook -> Order -> OrderBook
addOrder book order =
  let orderMap = HM.insert (ordId order) order (ordBookOrderMap book)
      limitPrice = ordPrice order
      askLimitMap = ordBookAskLimitMap book
      hasAskPriceLimit = HM.member limitPrice askLimitMap
      newAsklimitMap = if ordType order == Ask then insertOrUpdateLimitMap (askLimitMap) order else askLimitMap
      bidLimitMap = ordBookBidLimitMap book
      hasBidPriceLimit = HM.member limitPrice bidLimitMap
      newBidLimitMap = if ordType order == Bid then insertOrUpdateLimitMap bidLimitMap order else bidLimitMap
      askLimits = ordBookAskLimits book
      newAskLimits = if ordType order == Ask && (not hasAskPriceLimit) then H.insert limitPrice askLimits else askLimits
      bidLimits = ordBookBidLimits book
      newBidLimits = if ordType order == Bid && (not hasBidPriceLimit) then H.insert limitPrice bidLimits else bidLimits
      
      
  in book {
    ordBookOrderMap = orderMap,
    ordBookAskLimitMap = newAsklimitMap,
    ordBookBidLimitMap = newBidLimitMap,
    ordBookAskLimits = newAskLimits,
    ordBookBidLimits = newBidLimits,
    ordBookStats = incrementOrderCount $ ordBookStats book
  }
  where insertOrUpdateMap map k insertFun updateFun =
            HM.alter (\x ->maybe insertFun updateFun x) k map
        insertOrUpdateLimitMap m order =
            let limitPrice = ordPrice order
            in insertOrUpdateMap m limitPrice (Just $ singleLimit limitPrice (ordId order)) (\x -> Just $ appendOrder x (ordId order))


popOrderFromLimit :: Limit -> (Maybe OrderId, Maybe Limit)
popOrderFromLimit limit =
  let orders = lmtOrders limit
  in if null orders
     then (Nothing, Nothing)
     else (Just $ head orders, if (null $ tail orders) then Nothing else Just $ limit { lmtOrders = tail orders})
    

popOrderFromLimitMap :: HM.Map Integer Limit -> Integer -> (Maybe OrderId, HM.Map Integer Limit)
popOrderFromLimitMap limitMap price = 
  let orderId = maybe Nothing (\l -> fst $ popOrderFromLimit l) (HM.lookup price limitMap)
      newMap = HM.update (\limit -> snd $ popOrderFromLimit limit ) price limitMap
  in (orderId, newMap)

{-| Private function -}
{-
deleteOrderFromLimits b order = case (ordType order) of
  Ask -> let limitMap = deleteOrderFromLimitMap (ordBookAskLimitMap b) order
             limits = ordBookAskLimits b
             askLimits = popEmptyLimit limits limitMap
         in b {
           ordBookAskLimits = askLimits,
           ordBookAskLimitMap = limitMap}
  Bid ->let limitMap = deleteOrderFromLimitMap (ordBookBidLimitMap b) order
            limits = ordBookBidLimits b
            bidLimits = popEmptyLimit limits limitMap
        in b {
           ordBookBidLimits = bidLimits,
           ordBookBidLimitMap = limitMap
         }
-}
{-| Private function -}
deleteOrderFromMap b orderId = let map = HM.delete orderId (ordBookOrderMap b)
                               in b { ordBookOrderMap = map }

--popOrder ::  -> (Book, Maybe OrderId)
popOrder limit limitMap =
  let mprice = H.viewHead limit
  in case mprice of
    Nothing -> (limit, limitMap, Nothing)
    Just price -> let (mOrderId, newLimitMap) = popOrderFromLimitMap limitMap price
                      newLimit = popEmptyLimit limit newLimitMap
                  in (newLimit, newLimitMap, mOrderId)

popBidOrder :: OrderBook -> (Maybe OrderId, OrderBook)
popBidOrder b =
  let limit = ordBookBidLimits b
      limitMap = ordBookBidLimitMap b
      (newLimit, newLimitMap, orderId) = popOrder limit limitMap
  in (orderId, b { ordBookBidLimits = newLimit, ordBookBidLimitMap = newLimitMap})
  

popAskOrder :: OrderBook -> (Maybe OrderId, OrderBook)
popAskOrder b = 
  let limit = ordBookAskLimits b
      limitMap = ordBookAskLimitMap b
      (newLimit, newLimitMap, orderId) = popOrder limit limitMap
  in (orderId, b { ordBookAskLimits = newLimit, ordBookAskLimitMap = newLimitMap})
 
                     

popEmptyLimit :: H.HeapItem pol Integer => H.Heap pol Integer -> HM.Map Integer a -> H.Heap pol Integer
popEmptyLimit limits limitMap =
  let m = H.view limits
  in case m of
    Nothing -> limits
    Just (price, tail) -> if isJust (HM.lookup price limitMap) 
                    then limits
                    else tail



{- 
cancelOrder :: OrderBook -> OrderId -> OrderBook  
cancelOrder book id =
  let maybeOrder = HM.lookup id (ordBookOrderMap book)
  in case maybeOrder of
    Nothing -> book
    Just order ->
      let
          orderMap = HM.delete (ordId order) (ordBookOrderMap book)
          limitPrice = ordPrice order
          askLimitMap = ordBookAskLimitMap book
          hasAskPriceLimit = HM.member limitPrice askLimitMap
          newAsklimitMap = if ordType order == Ask then deleteOrderFromLimitMap (askLimitMap) order else askLimitMap
          bidLimitMap = ordBookBidLimitMap book
          hasBidPriceLimit = HM.member limitPrice bidLimitMap
          newBidLimitMap = if ordType order == Bid then deleteOrderFromLimitMap bidLimitMap order else bidLimitMap
      in book {
        ordBookOrderMap = orderMap,
        ordBookAskLimitMap = newAsklimitMap,
        ordBookBidLimitMap = newBidLimitMap
      }
-}   

data Trade = Trade {
  trdAsk :: Order,
  trdBid :: Order
}
  deriving (Eq, Ord, Read, Show)


getFirstOrderId limit = if isEmptyLimit limit then Nothing else Just $ head $ lmtOrders limit


execOrder :: OrderBook -> (OrderBook, Maybe Trade)
execOrder book
  | not (isPriceCrossed book) = (book, Nothing)
  | isPriceCrossed book && not (areBothJust $ getBestOrderId book)  = (book, Nothing) {- TODO: fix this case - remove empty limits -}
  | otherwise = let (mBidOrderId, book1) = popBidOrder book
                    (mAskOrderId, book2) = popAskOrder book1
                    (bidOrderId, askOrderId) = (fromJust mBidOrderId, fromJust mAskOrderId)
                    orderMap = ordBookOrderMap book
                    trade = Trade (fromJust $ HM.lookup askOrderId orderMap)
                            (fromJust $ HM.lookup bidOrderId orderMap)
                    newBook = deleteOrderFromMap (deleteOrderFromMap book2 bidOrderId) askOrderId
                in (newBook { ordBookStats = incrementTradeCount $ ordBookStats newBook }, Just trade)
  where isPriceCrossed book = maybe False id (priceCrossed book)
        priceCrossed book = do
          bidPrice <- getBestBid book
          askPrice <- getBestAsk book
          return $ askPrice <= bidPrice


getOrderId book id = HM.lookup id (ordBookOrderMap book)

areBothJust (Just _, Just _) = True
areBothJust _ = False

getBestOrderId book = (getBestBidOrderId book, getBestAskOrderId book)

getBestBidOrderId book = do
  price <- getBestBid book
  limit <- HM.lookup price (ordBookBidLimitMap book)
  getFirstOrderId limit

getBestAskOrderId book = do
  price <- getBestAsk book
  limit <- HM.lookup price (ordBookAskLimitMap book)
  getFirstOrderId limit


 
getBestBid book = H.viewHead (ordBookBidLimits book)
getBestAsk book = H.viewHead (ordBookAskLimits book)

emptyBook :: TimeSpec -> OrderBook
emptyBook created = OrderBook
  (HM.empty) (HM.empty) (HM.empty) (H.empty) (H.empty) (emptyStats created)
 
{-| Return true if limit has no orders -}
isEmptyLimit limit = null $ lmtOrders limit 
{-| Creates limit with single order with orderId -}
singleLimit price orderId = Limit price [orderId]

{-| Append order to limit -}
appendOrder limit orderId = limit {lmtOrders = (lmtOrders limit) ++ [orderId]}

 
