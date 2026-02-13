package biz

import (
	"bytes"
	pb "cardbinance/api/user/v1"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/go-kratos/kratos/v2/errors"
	"github.com/go-kratos/kratos/v2/log"
	transporthttp "github.com/go-kratos/kratos/v2/transport/http"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type User struct {
	ID               uint64
	Address          string
	Card             string
	CardNumber       string
	CardOrderId      string
	CardAmount       float64
	Amount           float64
	AmountTwo        uint64
	MyTotalAmount    uint64
	IsDelete         uint64
	Vip              uint64
	FirstName        string
	LastName         string
	Email            string
	CountryCode      string
	Phone            string
	PhoneCountryCode string
	State            string
	City             string
	Country          string
	Street           string
	PostalCode       string
	BirthDate        string
	CardUserId       string
	ProductId        string
	MaxCardQuota     uint64
	UserCount        uint64
	CreatedAt        time.Time
	UpdatedAt        time.Time
	VipTwo           uint64
	VipThree         uint64
	CardTwo          uint64
	CanVip           uint64
	CardTwoNumber    string
	IdCard           string
	Gender           string
	Pic              string
	PicTwo           string
	CardNumberRel    string
	CardNumberRelTwo string
}

type CardOrder struct {
	ID        uint64
	Last      uint64
	Code      string
	Card      string
	Time      *time.Time
	CreatedAt time.Time
	UpdatedAt time.Time
}

type UserRecommend struct {
	ID            uint64
	UserId        uint64
	RecommendCode string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

type Config struct {
	ID      uint64
	KeyName string
	Name    string
	Value   string
}

type Withdraw struct {
	ID        int64
	UserId    int64
	Amount    float64
	RelAmount float64
	Status    string
	Address   string
	CreatedAt time.Time
	UpdatedAt time.Time
}

type Reward struct {
	ID        uint64
	UserId    uint64
	Amount    float64
	Reason    uint64
	CreatedAt time.Time
	UpdatedAt time.Time
	Address   string
	One       uint64
}

type CardRecord struct {
	ID         uint64
	UserId     uint64
	RecordType uint64
	Remark     string
	Code       string
	Opt        string
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

type UserRepo interface {
	SetNonceByAddress(ctx context.Context, wallet string) (int64, error)
	GetAndDeleteWalletTimestamp(ctx context.Context, wallet string) (string, error)
	SetLockAmountToCardByAddress(ctx context.Context, wallet string) error
	GetLockAmountToCardByAddress(ctx context.Context, wallet string) (string, error)
	GetConfigByKeys(keys ...string) ([]*Config, error)
	GetUserByAddress(address string) (*User, error)
	GetUserById(userId uint64) (*User, error)
	GetUserRecommendByUserId(userId uint64) (*UserRecommend, error)
	CreateUser(ctx context.Context, uc *User) (*User, error)
	CreateUserRecommend(ctx context.Context, userId uint64, recommendUser *UserRecommend) (*UserRecommend, error)
	GetUserRecommendByCode(code string) ([]*UserRecommend, error)
	GetUserRecommendLikeCode(code string) ([]*UserRecommend, error)
	GetUserByUserIds(userIds []uint64) (map[uint64]*User, error)
	CreateCard(ctx context.Context, userId uint64, user *User) error
	UpdateCardCardNumberRel(ctx context.Context, userId uint64, cardNumberRel string) error
	UpdateCardCardNumberRelTwo(ctx context.Context, userId uint64, cardNumberRel string) error
	CreateCardTwo(ctx context.Context, userId uint64, user *User) error
	GetAllUsers() ([]*User, error)
	UpdateCard(ctx context.Context, userId uint64, cardOrderId, card string) error
	CreateCardRecommend(ctx context.Context, userId uint64, amount float64, vip uint64, address string) error
	AmountToCard(ctx context.Context, userId uint64, amount float64, amountRel float64, one uint64) (uint64, error)
	AmountToCardReward(ctx context.Context, userId uint64, amount float64, orderId string, rewardId uint64, one uint64) error
	AmountTo(ctx context.Context, userId, toUserId uint64, toAddress string, amount float64) error
	Withdraw(ctx context.Context, userId uint64, amount, amountRel float64, address string) error
	GetUserRewardByUserIdPage(ctx context.Context, b *Pagination, userId uint64, reason uint64, cardType uint64) ([]*Reward, error, int64)
	GetUserRecordByUserIdPage(ctx context.Context, b *Pagination, userId uint64) ([]*CardRecord, error, int64)
	SetVip(ctx context.Context, userId uint64, vip uint64) error
	GetUsersOpenCard() ([]*User, error)
	UploadCardPicTwo(ctx context.Context, userId uint64, pic string) error
	UploadCardPic(ctx context.Context, userId uint64, pic string) error
	UploadCardChangeTwo(ctx context.Context, userId uint64) error
	UploadCardChange(ctx context.Context, userId uint64) error
	UploadCardOneLock(ctx context.Context, userId uint64) error
	UploadCardTwoLock(ctx context.Context, userId uint64) error
	GetUserCodePage(ctx context.Context, b *Pagination, card string) ([]*CardOrder, error, int64)
}

type UserUseCase struct {
	repo UserRepo
	tx   Transaction
	log  *log.Helper
}

func NewUserUseCase(repo UserRepo, tx Transaction, logger log.Logger) *UserUseCase {
	return &UserUseCase{
		repo: repo,
		tx:   tx,
		log:  log.NewHelper(logger),
	}
}

func (uuc *UserUseCase) GetUserById(ctx context.Context, userId uint64) (*pb.GetUserReply, error) {
	var (
		user                   *User
		userRecommend          *UserRecommend
		userRecommendUser      *User
		myUserRecommendUserId  uint64
		myUserRecommendAddress string
		err                    error
		withdrawRate           float64
		amountToRate           float64
		cardTwo                string
	)

	var (
		configs []*Config
	)

	// 配置
	configs, err = uuc.repo.GetConfigByKeys("withdraw_rate")
	if nil != configs {
		for _, vConfig := range configs {
			if "withdraw_rate" == vConfig.KeyName {
				withdrawRate, _ = strconv.ParseFloat(vConfig.Value, 10)
			}
			if "amount_to_rate" == vConfig.KeyName {
				amountToRate, _ = strconv.ParseFloat(vConfig.Value, 10)
			}
			if "card_two" == vConfig.KeyName {
				cardTwo = vConfig.Value
			}
		}
	}

	user, err = uuc.repo.GetUserById(userId)
	if nil == user || nil != err {
		return &pb.GetUserReply{Status: "-1"}, nil
	}

	// 推荐
	userRecommend, err = uuc.repo.GetUserRecommendByUserId(userId)
	if nil == userRecommend {
		return &pb.GetUserReply{Status: "-1"}, nil
	}

	if "" != userRecommend.RecommendCode {
		tmpRecommendUserIds := strings.Split(userRecommend.RecommendCode, "D")
		if 2 <= len(tmpRecommendUserIds) {
			myUserRecommendUserId, _ = strconv.ParseUint(tmpRecommendUserIds[len(tmpRecommendUserIds)-1], 10, 64) // 最后一位是直推人

			if 0 < myUserRecommendUserId {
				userRecommendUser, err = uuc.repo.GetUserById(myUserRecommendUserId)
				if nil == userRecommendUser || nil != err {
					return &pb.GetUserReply{Status: "-1"}, nil
				}

				myUserRecommendAddress = userRecommendUser.Address
			}
		}
	}

	cardStatus := uint64(0)
	var (
		cardAmount    string
		cardAmountTwo string
	)
	if "no" == user.CardOrderId {
		cardStatus = 0
	} else {
		if "no" == user.CardNumber {
			cardStatus = 1
		} else {
			cardStatus = 2
			//// 查询状态。成功分红
			//var (
			//	resCard *CardInfoResponse
			//)
			//resCard, err = GetCardInfoRequestWithSign(user.Card)
			//if nil == resCard || 200 != resCard.Code || err != nil {
			//
			//} else {
			//	if "ACTIVE" == resCard.Data.CardStatus {
			//		cardAmount = resCard.Data.Balance
			//	}
			//}
		}
	}

	if 10 < len(user.CardNumber) {
		var (
			res *InterlaceCardSummaryResp
		)
		res, _ = InterlaceGetCardSummary(ctx, interlaceAccountId, user.CardNumber)
		if nil != res {
			cardAmount = res.Data.Balance.Available
		}
	}

	if 10 < len(user.CardTwoNumber) {
		var (
			res *InterlaceCardSummaryResp
		)
		res, _ = InterlaceGetCardSummary(ctx, interlaceAccountId, user.CardTwoNumber)
		if nil != res {
			cardAmountTwo = res.Data.Balance.Available
		}
	}

	return &pb.GetUserReply{
		Status:           "ok",
		Address:          user.Address,
		Amount:           fmt.Sprintf("%.2f", user.Amount),
		MyTotalAmount:    user.MyTotalAmount,
		Vip:              user.Vip,
		CardNum:          "",
		CardStatus:       cardStatus,
		CardAmount:       cardAmount,
		RecommendAddress: myUserRecommendAddress,
		WithdrawRate:     withdrawRate,
		CardStatusTwo:    user.CardTwo,
		CanVip:           user.CanVip,
		VipThree:         user.VipThree,
		CardTwo:          cardTwo,
		CardAmountTwo:    cardAmountTwo,
		PicTwo:           "/images/" + user.PicTwo,
		Pic:              "/images/" + user.Pic,
		AmountToRate:     amountToRate,
	}, nil
}

func (uuc *UserUseCase) GetUserDataById(userId uint64) (*User, error) {
	return uuc.repo.GetUserById(userId)
}

func (uuc *UserUseCase) GetUserRecommend(ctx context.Context, req *pb.RecommendListRequest) (*pb.RecommendListReply, error) {
	var (
		userRecommend   *UserRecommend
		myUserRecommend []*UserRecommend
		user            *User
		err             error
	)

	res := make([]*pb.RecommendListReply_List, 0)

	if 0 >= len(req.Address) {
		return &pb.RecommendListReply{
			Status:     "错误",
			Recommends: res,
		}, nil
	}

	user, err = uuc.repo.GetUserByAddress(req.Address)
	if nil == user || nil != err {
		return &pb.RecommendListReply{
			Status:     "错误",
			Recommends: res,
		}, nil
	}

	// 推荐
	userRecommend, err = uuc.repo.GetUserRecommendByUserId(user.ID)
	if nil == userRecommend {
		return &pb.RecommendListReply{
			Status:     "错误",
			Recommends: res,
		}, nil
	}

	myUserRecommend, err = uuc.repo.GetUserRecommendByCode(userRecommend.RecommendCode + "D" + strconv.FormatUint(user.ID, 10))
	if nil == myUserRecommend || nil != err {
		return &pb.RecommendListReply{
			Status:     "错误",
			Recommends: res,
		}, nil
	}

	if 0 >= len(myUserRecommend) {
		return &pb.RecommendListReply{
			Status:     "ok",
			Recommends: res,
		}, nil
	}

	tmpUserIds := make([]uint64, 0)
	for _, vMyUserRecommend := range myUserRecommend {
		tmpUserIds = append(tmpUserIds, vMyUserRecommend.UserId)
	}
	if 0 >= len(tmpUserIds) {
		return &pb.RecommendListReply{
			Status:     "错误",
			Recommends: res,
		}, nil
	}

	var (
		usersMap map[uint64]*User
	)

	usersMap, err = uuc.repo.GetUserByUserIds(tmpUserIds)
	if nil == usersMap || nil != err {
		return &pb.RecommendListReply{
			Status:     "错误",
			Recommends: res,
		}, nil
	}

	if 0 >= len(usersMap) {
		return &pb.RecommendListReply{
			Status:     "错误",
			Recommends: res,
		}, nil
	}

	for _, vMyUserRecommend := range myUserRecommend {
		if _, ok := usersMap[vMyUserRecommend.UserId]; !ok {
			continue
		}

		cardOpen := uint64(0)
		if "no" != usersMap[vMyUserRecommend.UserId].CardNumber {
			cardOpen = 1
		}

		res = append(res, &pb.RecommendListReply_List{
			Address:  usersMap[vMyUserRecommend.UserId].Address,
			Amount:   usersMap[vMyUserRecommend.UserId].AmountTwo + usersMap[vMyUserRecommend.UserId].MyTotalAmount,
			Vip:      usersMap[vMyUserRecommend.UserId].Vip,
			VipThree: usersMap[vMyUserRecommend.UserId].VipThree,
			CardOpen: cardOpen,
		})
	}

	return &pb.RecommendListReply{
		Status:     "ok",
		Recommends: res,
	}, nil
}

type Pagination struct {
	PageNum  int
	PageSize int
}

func (uuc *UserUseCase) OrderList(ctx context.Context, req *pb.OrderListRequest, userId uint64) (*pb.OrderListReply, error) {
	res := make([]*pb.OrderListReply_List, 0)

	var (
		user  *User
		err   error
		total uint64
	)

	user, err = uuc.repo.GetUserById(userId)
	if nil == user || nil != err {
		return &pb.OrderListReply{Status: "查询错误", Count: 0,
			List: res,
		}, nil
	}

	if 1 == req.CardType {
		if 10 > len(user.CardTwoNumber) {
			return &pb.OrderListReply{Status: "ok", Count: 0,
				List: res,
			}, nil
		}

		txs, totalTmp, errTwo := InterlaceListTransactions(ctx, &InterlaceTxnListReq{
			AccountId: interlaceAccountId,
			CardId:    user.CardTwoNumber,
			Limit:     20,
			Page:      int(req.Page),
			// StartTime: "1735689600000",
			// EndTime:   "1738272000000",
		})
		if errTwo != nil {
			return &pb.OrderListReply{Status: "ok", Count: 0,
				List: res,
			}, nil
		}

		total, _ = strconv.ParseUint(totalTmp, 10, 64)

		if nil != txs {
			for _, v := range txs {
				tmpStatus := "SUCCESS"
				if "CLOSED" == v.Status {

				} else if "FAIL" == v.Status {
					tmpStatus = "FAILED"
				} else if "PENDING" == v.Status {
					tmpStatus = "PROCESSING"
				}

				if 3 == v.Type {
					continue
				}

				res = append(res, &pb.OrderListReply_List{
					Timestamp:               v.CreateTime,
					Status:                  tmpStatus,
					TradeAmount:             v.Amount,
					ActualTransactionAmount: v.TransactionAmount,
					ServiceFee:              v.Fee,
					TradeDescription:        v.TransactionAmount,
					CurrentBalance:          "暂未获取",
					TraderNum:               v.Detail,
				})
			}
		}
	} else {
		if 10 > len(user.CardNumber) {
			return &pb.OrderListReply{Status: "ok", Count: 0,
				List: res,
			}, nil
		}

		txs, totalTmp, errTwo := InterlaceListTransactions(ctx, &InterlaceTxnListReq{
			AccountId: interlaceAccountId,
			CardId:    user.CardNumber,
			Limit:     20,
			Page:      int(req.Page),
			// StartTime: "1735689600000",
			// EndTime:   "1738272000000",
		})
		if errTwo != nil {
			return &pb.OrderListReply{Status: "ok", Count: 0,
				List: res,
			}, nil
		}

		total, _ = strconv.ParseUint(totalTmp, 10, 64)

		if nil != txs {
			for _, v := range txs {
				tmpStatus := "SUCCESS"
				if "CLOSED" == v.Status {

				} else if "FAIL" == v.Status {
					tmpStatus = "FAILED"
				} else if "PENDING" == v.Status {
					tmpStatus = "PROCESSING"
				}

				if 3 == v.Type {
					continue
				}

				res = append(res, &pb.OrderListReply_List{
					Timestamp:               v.CreateTime,
					Status:                  tmpStatus,
					TradeAmount:             v.Amount,
					ActualTransactionAmount: v.TransactionAmount,
					ServiceFee:              v.Fee,
					TradeDescription:        v.TransactionAmount,
					CurrentBalance:          "暂未获取",
					TraderNum:               v.Detail,
				})
			}
		}
	}

	return &pb.OrderListReply{
		Status: "ok",
		Count:  total,
		List:   res,
	}, nil
}

func (uuc *UserUseCase) OrderListTwo(ctx context.Context, req *pb.OrderListTwoRequest, userId uint64) (*pb.OrderListTwoReply, error) {
	res := make([]*pb.OrderListTwoReply_List, 0)

	var (
		user  *User
		err   error
		total uint64
	)

	user, err = uuc.repo.GetUserById(userId)
	if nil == user || nil != err {
		return &pb.OrderListTwoReply{Status: "查询错误", Count: 0,
			List: res,
		}, nil
	}

	if 1 == req.CardType {
		if 10 > len(user.CardTwoNumber) {
			return &pb.OrderListTwoReply{Status: "ok", Count: 0,
				List: res,
			}, nil
		}

		txs, totalTmp, errTwo := InterlaceListTransactions(ctx, &InterlaceTxnListReq{
			AccountId: interlaceAccountId,
			CardId:    user.CardTwoNumber,
			Limit:     20,
			Page:      int(req.Page),
			Type:      "1",
			// StartTime: "1735689600000",
			// EndTime:   "1738272000000",
		})
		if errTwo != nil {
			return &pb.OrderListTwoReply{Status: "ok", Count: 0,
				List: res,
			}, nil
		}

		total, _ = strconv.ParseUint(totalTmp, 10, 64)

		if nil != txs {
			for _, v := range txs {
				res = append(res, &pb.OrderListTwoReply_List{
					Timestamp:   v.CreateTime,
					Status:      v.Status,
					TradeAmount: v.TransactionAmount,
					Remark:      v.Remark,
					Detail:      v.Detail,
					ServiceFee:  v.Fee,
				})
			}
		}
	} else {
		if 10 > len(user.CardNumber) {
			return &pb.OrderListTwoReply{Status: "ok", Count: 0,
				List: res,
			}, nil
		}

		txs, totalTmp, errTwo := InterlaceListTransactions(ctx, &InterlaceTxnListReq{
			AccountId: interlaceAccountId,
			CardId:    user.CardNumber,
			Limit:     20,
			Page:      int(req.Page),
			Type:      "1",
			// StartTime: "1735689600000",
			// EndTime:   "1738272000000",
		})
		if errTwo != nil {
			return &pb.OrderListTwoReply{Status: "ok", Count: 0,
				List: res,
			}, nil
		}

		total, _ = strconv.ParseUint(totalTmp, 10, 64)

		if nil != txs {
			for _, v := range txs {
				res = append(res, &pb.OrderListTwoReply_List{
					Timestamp:   v.CreateTime,
					Status:      v.Status,
					TradeAmount: v.TransactionAmount,
					Remark:      v.Remark,
					Detail:      v.Detail,
					ServiceFee:  v.Fee,
				})
			}
		}
	}

	return &pb.OrderListTwoReply{
		Status: "ok",
		Count:  total,
		List:   res,
	}, nil
}

func (uuc *UserUseCase) RecordList(ctx context.Context, req *pb.RecordListRequest, userId uint64) (*pb.RecordListReply, error) {
	res := make([]*pb.RecordListReply_List, 0)

	var (
		userRewards []*CardRecord
		count       int64
		err         error
	)

	userRewards, err, count = uuc.repo.GetUserRecordByUserIdPage(ctx, &Pagination{
		PageNum:  int(req.Page),
		PageSize: 20,
	}, userId)
	if nil != err {
		return &pb.RecordListReply{
			Status: "ok",
			Count:  uint64(count),
			List:   res,
		}, err
	}

	for _, vUserReward := range userRewards {
		res = append(res, &pb.RecordListReply_List{
			CreatedAt: vUserReward.CreatedAt.Add(8 * time.Hour).Format("2006-01-02 15:04:05"),
			Remark:    vUserReward.Remark,
		})
	}

	return &pb.RecordListReply{
		Status: "ok",
		Count:  uint64(count),
		List:   res,
	}, nil
}

func (uuc *UserUseCase) RewardList(ctx context.Context, req *pb.RewardListRequest, userId uint64) (*pb.RewardListReply, error) {
	res := make([]*pb.RewardListReply_List, 0)

	var (
		userRewards []*Reward
		count       int64
		err         error
	)

	if 1 > req.ReqType || 11 < req.ReqType {
		return &pb.RewardListReply{
			Status: "参数错误",
			Count:  0,
			List:   res,
		}, nil
	}

	userRewards, err, count = uuc.repo.GetUserRewardByUserIdPage(ctx, &Pagination{
		PageNum:  int(req.Page),
		PageSize: 20,
	}, userId, req.ReqType, req.CardType)
	if nil != err {
		return &pb.RewardListReply{
			Status: "ok",
			Count:  uint64(count),
			List:   res,
		}, err
	}

	for _, vUserReward := range userRewards {
		res = append(res, &pb.RewardListReply_List{
			CreatedAt: vUserReward.CreatedAt.Add(8 * time.Hour).Format("2006-01-02 15:04:05"),
			Amount:    fmt.Sprintf("%.4f", vUserReward.Amount),
			Address:   vUserReward.Address,
		})
	}

	return &pb.RewardListReply{
		Status: "ok",
		Count:  uint64(count),
		List:   res,
	}, nil
}

// 无锁的

func (uuc *UserUseCase) GetExistUserByAddressOrCreate(ctx context.Context, u *User, req *pb.EthAuthorizeRequest) (*User, error, string) {
	var (
		user          *User
		recommendUser *UserRecommend
		err           error
		//configs       []*Config
		//vipMax        uint64
	)

	// 配置
	//configs, err = uuc.repo.GetConfigByKeys("vip_max")
	//if nil != configs {
	//	for _, vConfig := range configs {
	//		if "vip_max" == vConfig.KeyName {
	//			vipMax, _ = strconv.ParseUint(vConfig.Value, 10, 64)
	//		}
	//	}
	//}

	recommendUser = &UserRecommend{
		ID:            0,
		UserId:        0,
		RecommendCode: "",
	}

	user, err = uuc.repo.GetUserByAddress(u.Address) // 查询用户
	if nil == user && nil == err {
		code := req.SendBody.Code // 查询推荐码 abf00dd52c08a9213f225827bc3fb100 md5 dhbmachinefirst
		if "abf00dd52c08a9213f225827bc3fb100" != code {
			if 1 >= len(code) {
				return nil, errors.New(500, "USER_ERROR", "无效的推荐码1"), "无效的推荐码"
			}
			var (
				userRecommend *User
			)

			userRecommend, err = uuc.repo.GetUserByAddress(code)
			if nil == userRecommend || err != nil {
				return nil, errors.New(500, "USER_ERROR", "无效的推荐码1"), "无效的推荐码"
			}

			// 查询推荐人的相关信息
			recommendUser, err = uuc.repo.GetUserRecommendByUserId(userRecommend.ID)
			if nil == recommendUser || err != nil {
				return nil, errors.New(500, "USER_ERROR", "无效的推荐码3"), "无效的推荐码3"
			}
		} else {
			u.Vip = 15
		}

		if err = uuc.tx.ExecTx(ctx, func(ctx context.Context) error { // 事务
			user, err = uuc.repo.CreateUser(ctx, u) // 用户创建
			if err != nil {
				return err
			}

			_, err = uuc.repo.CreateUserRecommend(ctx, user.ID, recommendUser) // 创建用户推荐信息
			if err != nil {
				return err
			}

			return nil
		}); err != nil {
			return nil, err, "错误"
		}
	}

	return user, err, ""
}

// 有锁的

var lockCreateNonce sync.Mutex

func (uuc *UserUseCase) CreateNonce(ctx context.Context, req *pb.CreateNonceRequest) (*pb.CreateNonceReply, error) {
	lockCreateNonce.Lock()
	defer lockCreateNonce.Unlock()

	nonce, err := uuc.repo.SetNonceByAddress(ctx, req.SendBody.Address)
	if nil != err {
		return &pb.CreateNonceReply{Nonce: "-1", Status: "生成错误"}, err
	}

	return &pb.CreateNonceReply{Nonce: strconv.FormatInt(nonce, 10), Status: "ok"}, nil
}

// 凡是操作的都涉及到这个锁
var lockNonce sync.Mutex

func (uuc *UserUseCase) GetAddressNonce(ctx context.Context, address string) (string, error) {
	lockNonce.Lock()
	defer lockNonce.Unlock()

	return uuc.repo.GetAndDeleteWalletTimestamp(ctx, address)
}

var lockVip sync.Mutex

func (uuc *UserUseCase) SetVip(ctx context.Context, req *pb.SetVipRequest, userId uint64) (*pb.SetVipReply, error) {
	lockVip.Lock()
	defer lockVip.Unlock()

	var (
		user   *User
		toUser *User
		err    error
	)

	user, err = uuc.repo.GetUserById(userId)
	if nil == user || nil != err {
		return &pb.SetVipReply{Status: "用户不存在"}, nil
	}

	toUser, err = uuc.repo.GetUserByAddress(req.SendBody.Address)
	if nil == toUser || nil != err {
		return &pb.SetVipReply{Status: "目标用户不存在"}, nil
	}

	if 0 > req.SendBody.Vip || 14 < req.SendBody.Vip {
		return &pb.SetVipReply{Status: "vip等级必须在0-14之间"}, nil
	}

	if req.SendBody.Vip >= user.Vip {
		return &pb.SetVipReply{Status: "必须小于自己的vip等级"}, nil
	}

	if 30 > len(req.SendBody.Address) || 60 < len(req.SendBody.Address) {
		return &pb.SetVipReply{Status: "账号参数格式不正确"}, nil
	}

	if req.SendBody.Vip == toUser.Vip {
		return &pb.SetVipReply{Status: "无需修改"}, nil
	}

	var (
		users    []*User
		usersMap map[uint64]*User
	)
	users, err = uuc.repo.GetAllUsers()
	if nil == users {
		return &pb.SetVipReply{Status: "获取数据错误不存在"}, nil
	}

	usersMap = make(map[uint64]*User, 0)
	for _, vUsers := range users {
		usersMap[vUsers.ID] = vUsers
	}

	var (
		userRecommend   *UserRecommend
		myUserRecommend []*UserRecommend
	)
	// 推荐
	userRecommend, err = uuc.repo.GetUserRecommendByUserId(toUser.ID)
	if nil == userRecommend {
		return &pb.SetVipReply{Status: "目标用户不存在"}, nil
	}

	if 1 == user.CanVip {
		if "" != userRecommend.RecommendCode {
			tmpRecommendUserIds := strings.Split(userRecommend.RecommendCode, "D")
			if 2 <= len(tmpRecommendUserIds) {
				tmpMyUp := false
				for _, v := range tmpRecommendUserIds {
					myUserRecommendUserId, _ := strconv.ParseUint(v, 10, 64) // 最后一位是直推人
					if myUserRecommendUserId <= 0 {
						continue
					} else {
						if myUserRecommendUserId == userId {
							tmpMyUp = true
						}
					}
				}

				if !tmpMyUp {
					return &pb.SetVipReply{Status: "目标用户并不是你的团队用户"}, nil
				}
			}
		} else {
			return &pb.SetVipReply{Status: "目标用户无上级"}, nil
		}

	} else {
		if "" != userRecommend.RecommendCode {
			tmpRecommendUserIds := strings.Split(userRecommend.RecommendCode, "D")
			if 2 <= len(tmpRecommendUserIds) {
				myUserRecommendUserId, _ := strconv.ParseUint(tmpRecommendUserIds[len(tmpRecommendUserIds)-1], 10, 64) // 最后一位是直推人
				if myUserRecommendUserId <= 0 || myUserRecommendUserId != userId {
					return &pb.SetVipReply{Status: "不是直推下级用户"}, nil
				}
			}
		} else {
			return &pb.SetVipReply{Status: "目标用户无上级"}, nil
		}

		// 下级比我小
		myUserRecommend, err = uuc.repo.GetUserRecommendLikeCode(userRecommend.RecommendCode + "D" + strconv.FormatUint(toUser.ID, 10))
		if nil == myUserRecommend || nil != err {
			return &pb.SetVipReply{Status: "获取数据错误不存在"}, nil
		}

		for _, v := range myUserRecommend {
			if _, ok := usersMap[v.UserId]; !ok {
				return &pb.SetVipReply{Status: "数据异常"}, nil
			}

			if req.SendBody.Vip <= usersMap[v.UserId].Vip {
				return &pb.SetVipReply{Status: "他下级的等级存在大于等于当前的设置"}, nil
			}
		}
	}

	if err = uuc.tx.ExecTx(ctx, func(ctx context.Context) error { // 事务
		err = uuc.repo.SetVip(ctx, toUser.ID, req.SendBody.Vip)
		if nil != err {
			return err
		}

		return nil
	}); nil != err {
		fmt.Println(err, "设置vip写入mysql错误", user)
		return &pb.SetVipReply{
			Status: "设置vip错误，联系管理员",
		}, nil
	}

	return &pb.SetVipReply{
		Status: "ok",
	}, nil
}

func (uuc *UserUseCase) OpenCard(ctx context.Context, req *pb.OpenCardRequest, userId uint64) (*pb.OpenCardReply, error) {
	var (
		user       *User
		err        error
		cardAmount float64
	)

	user, err = uuc.repo.GetUserById(userId)
	if nil == user || nil != err {
		return &pb.OpenCardReply{Status: "用户不存在"}, nil
	}

	if 5 <= user.UserCount {
		return &pb.OpenCardReply{Status: "提交已经5次。联系管理员"}, nil
	}

	if "no" != user.CardOrderId {
		return &pb.OpenCardReply{Status: "已经提交开卡信息"}, nil
	}

	//if "no" != user.CardNumber {
	//	return &pb.OpenCardReply{Status: "已经开卡"}, nil
	//}

	if 15 > uint64(user.Amount) {
		return &pb.OpenCardReply{Status: "账号余额不足15u"}, nil
	}
	cardAmount = 15

	if 1 > len(req.SendBody.Email) || len(req.SendBody.Email) > 99 {
		return &pb.OpenCardReply{Status: "邮箱错误"}, nil
	}

	//var (
	//	HolderID        string
	//	productIdUseTwo string
	//	maxCardQuotaTwo uint64
	//)
	//if 5 < len(user.CardUserId) {
	//HolderID = user.CardUserId
	//productIdUseTwo = user.ProductId
	//maxCardQuotaTwo = user.MaxCardQuota
	//var productIdUseInt64 uint64
	//productIdUseInt64, err = strconv.ParseUint(user.ProductId, 10, 64)
	//if nil != err || 0 >= productIdUseInt64 {
	//	return &pb.OpenCardReply{Status: "获取产品信息错误"}, nil
	//}
	//
	//// 请求
	//var (
	//	resCreatCardholder *CreateCardholderResponse
	//)
	//resCreatCardholder, err = UpdateCardholderRequest(productIdUseInt64, &User{
	//	CardUserId:  HolderID,
	//	FirstName:   req.SendBody.FirstName,
	//	LastName:    req.SendBody.LastName,
	//	Email:       user.Email,
	//	CountryCode: req.SendBody.CountryCode,
	//	Phone:       req.SendBody.Phone,
	//	City:        req.SendBody.City,
	//	Country:     req.SendBody.Country,
	//	Street:      req.SendBody.Street,
	//	PostalCode:  req.SendBody.PostalCode,
	//	BirthDate:   req.SendBody.BirthDate,
	//})
	//if nil == resCreatCardholder || err != nil {
	//	fmt.Println("持卡人订单创建失败:", user, resCreatCardholder, err)
	//	return &pb.OpenCardReply{Status: "请求创建持卡人系统错误"}, nil
	//}
	//if 200 != resCreatCardholder.Code {
	//	fmt.Println("请求创建持卡人系统错误", user, resCreatCardholder, err)
	//	return &pb.OpenCardReply{Status: "请求创建持卡人系统错误" + resCreatCardholder.Msg}, nil
	//}
	//
	//if 0 > len(resCreatCardholder.Data.HolderID) {
	//	fmt.Println("持卡人订单信息错误", user, resCreatCardholder, err)
	//	return &pb.OpenCardReply{Status: "请求创建持卡人系统错误，信息缺失"}, nil
	//}
	//
	//if err = uuc.tx.ExecTx(ctx, func(ctx context.Context) error { // 事务
	//	err = uuc.repo.CreateCard(ctx, userId, &User{
	//		Amount:       10,
	//		CardUserId:   HolderID,
	//		MaxCardQuota: maxCardQuotaTwo,
	//		ProductId:    productIdUseTwo,
	//		FirstName:    req.SendBody.FirstName,
	//		LastName:     req.SendBody.LastName,
	//		Email:        user.Email,
	//		CountryCode:  req.SendBody.CountryCode,
	//		Phone:        req.SendBody.Phone,
	//		City:         req.SendBody.City,
	//		Country:      req.SendBody.Country,
	//		Street:       req.SendBody.Street,
	//		PostalCode:   req.SendBody.PostalCode,
	//		BirthDate:    req.SendBody.BirthDate,
	//	})
	//	if nil != err {
	//		return err
	//	}
	//
	//	return nil
	//}); nil != err {
	//	fmt.Println(err, "开卡写入mysql错误", user)
	//	return &pb.OpenCardReply{
	//		Status: "开卡错误，联系管理员",
	//	}, nil
	//}

	//} else {
	//var (
	//products          *CardProductListResponse
	//productIdUse             = "1923750198816256002"
	//productIdUseInt64 uint64 = 1923750198816256002
	//maxCardQuota      uint64 = 100
	//)
	//products, err = GetCardProducts()
	//if nil == products || nil != err {
	//	//fmt.Println("产品信息错误1")
	//	return &pb.OpenCardReply{Status: "获取产品信息错误"}, nil
	//}
	//
	//for _, v := range products.Rows {
	//	if 0 < len(v.ProductId) && "ENABLED" == v.ProductStatus {
	//		productIdUse = v.ProductId
	//		maxCardQuota = v.MaxCardQuota
	//		productIdUseInt64, err = strconv.ParseUint(productIdUse, 10, 64)
	//		if nil != err {
	//			//fmt.Println("产品信息错误2")
	//			return &pb.OpenCardReply{Status: "获取产品信息错误"}, nil
	//		}
	//		//fmt.Println("当前选择产品信息", productIdUse, maxCardQuota, v)
	//		break
	//	}
	//}
	//
	//if 0 >= maxCardQuota {
	//	//fmt.Println("产品信息错误3")
	//	return &pb.OpenCardReply{Status: "获取产品信息错误,额度0"}, nil
	//}
	//
	//if 0 >= productIdUseInt64 {
	//	//fmt.Println("产品信息错误4")
	//	return &pb.OpenCardReply{Status: "获取产品信息错误,产品id0"}, nil
	//}

	// 请求
	//var (
	//	countryCode        = "CN"
	//	basePhone          = uint64(13077000000)
	//	resCreatCardholder *CreateCardholderResponse
	//)
	//
	//if 0 < user.UserCount {
	//	basePhone += user.UserCount * 100000
	//}
	//
	//phone := strconv.FormatUint(basePhone+userId, 10)
	//
	//resCreatCardholder, err = CreateCardholderRequest(productIdUseInt64, &User{
	//	FirstName: req.SendBody.FirstName,
	//	LastName:  req.SendBody.LastName,
	//	Email:     req.SendBody.Email,
	//	//CountryCode: req.SendBody.CountryCode,
	//	//Phone:       req.SendBody.Phone,
	//	CountryCode: countryCode,
	//	Phone:       phone,
	//	City:        req.SendBody.City,
	//	Country:     req.SendBody.Country,
	//	Street:      req.SendBody.Street,
	//	PostalCode:  req.SendBody.PostalCode,
	//	BirthDate:   req.SendBody.BirthDate,
	//})
	//if nil == resCreatCardholder || err != nil {
	//	fmt.Println("持卡人订单创建失败:", user, resCreatCardholder, err)
	//	return &pb.OpenCardReply{Status: "请求创建持卡人系统错误"}, nil
	//}
	//if 200 != resCreatCardholder.Code {
	//	fmt.Println("请求创建持卡人系统错误", user, resCreatCardholder, err)
	//	return &pb.OpenCardReply{Status: "请求创建持卡人系统错误" + resCreatCardholder.Msg}, nil
	//}
	//
	//if 0 > len(resCreatCardholder.Data.HolderID) {
	//	fmt.Println("持卡人订单信息错误", user, resCreatCardholder, err)
	//	return &pb.OpenCardReply{Status: "请求创建持卡人系统错误，信息缺失"}, nil
	//}
	//
	//fmt.Println("持卡人信息", user, resCreatCardholder)
	//HolderID = resCreatCardholder.Data.HolderID
	//maxCardQuotaTwo = maxCardQuota
	//productIdUseTwo = productIdUse

	if err = uuc.tx.ExecTx(ctx, func(ctx context.Context) error { // 事务
		err = uuc.repo.CreateCard(ctx, userId, &User{
			Amount: cardAmount,
		})
		if nil != err {
			return err
		}

		return nil
	}); nil != err {
		fmt.Println(err, "开卡写入mysql错误", user)
		return &pb.OpenCardReply{
			Status: "开卡错误，联系管理员",
		}, nil
	}
	//}

	return &pb.OpenCardReply{
		Status: "ok",
	}, nil
}

func (uuc *UserUseCase) CheckCard(ctx context.Context, req *pb.CheckCardRequest, userId uint64) (*pb.CheckCardReply, error) {
	var (
		user *User
		err  error
	)

	user, err = uuc.repo.GetUserById(userId)
	if nil == user || nil != err {
		return &pb.CheckCardReply{Status: "用户不存在"}, nil
	}

	if 2 == req.SendBody.CheckType {
		if 5 >= len(user.CardNumber) {
			return &pb.CheckCardReply{Status: "未提交虚拟卡开卡信息"}, nil
		}

		if 16 != len(req.SendBody.Num) {
			return &pb.CheckCardReply{Status: "卡号格式错误"}, nil
		}

		if err = uuc.tx.ExecTx(ctx, func(ctx context.Context) error { // 事务
			err = uuc.repo.UpdateCardCardNumberRel(ctx, userId, req.SendBody.Num)
			if nil != err {
				return err
			}

			return nil
		}); nil != err {
			fmt.Println(err, "开卡写入mysql错误", user)
			return &pb.CheckCardReply{
				Status: "开卡错误，联系管理员",
			}, nil
		}
	} else {
		if 16 != len(req.SendBody.Num) {
			return &pb.CheckCardReply{Status: "卡号格式错误"}, nil
		}

		if 2 == user.CardTwo {
			return &pb.CheckCardReply{Status: "已经激活卡片"}, nil
		}

		if err = uuc.tx.ExecTx(ctx, func(ctx context.Context) error { // 事务
			err = uuc.repo.UpdateCardCardNumberRelTwo(ctx, userId, req.SendBody.Num)
			if nil != err {
				return err
			}

			return nil
		}); nil != err {
			fmt.Println(err, "开卡写入mysql错误", user)
			return &pb.CheckCardReply{
				Status: "开卡错误，联系管理员",
			}, nil
		}
	}

	return &pb.CheckCardReply{
		Status: "ok",
	}, nil
}

func (uuc *UserUseCase) OpenCardTwo(ctx context.Context, req *pb.OpenCardRequest, userId uint64) (*pb.OpenCardReply, error) {
	var (
		user *User
		err  error
	)
	var (
		configs    []*Config
		cardAmount = float64(150)
	)

	// 配置
	configs, err = uuc.repo.GetConfigByKeys("card_two")
	if nil != configs {
		for _, vConfig := range configs {
			if "card_two" == vConfig.KeyName {
				cardAmount, _ = strconv.ParseFloat(vConfig.Value, 10)
			}
		}
	}

	user, err = uuc.repo.GetUserById(userId)
	if nil == user || nil != err {
		return &pb.OpenCardReply{Status: "用户不存在"}, nil
	}

	//if 4 >= len(user.Pic) || 4 >= len(user.PicTwo) {
	//	return &pb.OpenCardReply{Status: "先上传证件照片"}, nil
	//}

	if 5 <= user.UserCount {
		return &pb.OpenCardReply{Status: "提交已经5次。联系管理员"}, nil
	}

	if 0 < user.CardTwo {
		return &pb.OpenCardReply{Status: "已提交"}, nil
	}

	if uint64(cardAmount) > uint64(user.Amount) {
		return &pb.OpenCardReply{Status: "账号余额不足199u"}, nil
	}

	if 1 > len(req.SendBody.Email) || len(req.SendBody.Email) > 99 {
		return &pb.OpenCardReply{Status: "邮箱错误"}, nil
	}

	if 1 > len(req.SendBody.FirstName) || len(req.SendBody.FirstName) > 44 {
		return &pb.OpenCardReply{Status: "名字错误"}, nil
	}

	if 1 > len(req.SendBody.LastName) || len(req.SendBody.LastName) > 44 {
		return &pb.OpenCardReply{Status: "姓错误"}, nil
	}

	if 1 > len(req.SendBody.Phone) || len(req.SendBody.Phone) > 44 {
		return &pb.OpenCardReply{Status: "手机号错误"}, nil
	}

	if 1 > len(req.SendBody.CountryCode) || len(req.SendBody.CountryCode) > 44 {
		return &pb.OpenCardReply{Status: "国家代码错误"}, nil
	}

	if 1 > len(req.SendBody.Street) || len(req.SendBody.Street) > 99 {
		return &pb.OpenCardReply{Status: "街道错误"}, nil
	}

	if 1 > len(req.SendBody.City) || len(req.SendBody.City) > 99 {
		return &pb.OpenCardReply{Status: "城市错误"}, nil
	}

	if 1 > len(req.SendBody.PostalCode) || len(req.SendBody.PostalCode) > 99 {
		return &pb.OpenCardReply{Status: "邮政编码错误"}, nil
	}

	//if 1 > len(req.SendBody.PhoneCountryCode) || len(req.SendBody.PhoneCountryCode) > 99 {
	//	return &pb.OpenCardReply{Status: "手机号国家代码错误"}, nil
	//}

	if 1 > len(req.SendBody.Gender) || len(req.SendBody.Gender) > 40 {
		return &pb.OpenCardReply{Status: "性别错误"}, nil
	}

	if 10 > len(req.SendBody.IdCard) || len(req.SendBody.IdCard) > 40 {
		return &pb.OpenCardReply{Status: "身份证号码错误"}, nil
	}

	if err = uuc.tx.ExecTx(ctx, func(ctx context.Context) error { // 事务
		err = uuc.repo.CreateCardTwo(ctx, userId, &User{
			Amount:           cardAmount,
			FirstName:        req.SendBody.FirstName,
			LastName:         req.SendBody.LastName,
			Email:            req.SendBody.Email,
			CountryCode:      req.SendBody.CountryCode,
			Phone:            req.SendBody.Phone,
			City:             req.SendBody.City,
			Country:          req.SendBody.Country,
			Street:           req.SendBody.Street,
			PostalCode:       req.SendBody.PostalCode,
			State:            req.SendBody.State,
			PhoneCountryCode: "86",
			Gender:           req.SendBody.Gender,
			IdCard:           req.SendBody.IdCard,
		})
		if nil != err {
			return err
		}

		return nil
	}); nil != err {
		fmt.Println(err, "开卡2写入mysql错误", user)
		return &pb.OpenCardReply{
			Status: "开卡错误，联系管理员",
		}, nil
	}

	return &pb.OpenCardReply{
		Status: "ok",
	}, nil
}

func (uuc *UserUseCase) AmountToCard(ctx context.Context, req *pb.AmountToCardRequest, userId uint64) (*pb.AmountToCardReply, error) {
	var (
		user             *User
		err              error
		lockAmountToCard string
	)
	user, err = uuc.repo.GetUserById(userId)
	if nil == user || nil != err {
		return &pb.AmountToCardReply{Status: "用户不存在"}, nil
	}

	var (
		configs      []*Config
		amountToRate float64
	)

	// 配置
	configs, err = uuc.repo.GetConfigByKeys("amount_to_rate")
	if nil != configs {
		for _, vConfig := range configs {
			if "amount_to_rate" == vConfig.KeyName {
				amountToRate, _ = strconv.ParseFloat(vConfig.Value, 10)
			}
		}
	}

	lockAmountToCard, err = uuc.repo.GetLockAmountToCardByAddress(ctx, user.Address)
	if 0 < len(lockAmountToCard) {
		return &pb.AmountToCardReply{Status: "每分钟划转1笔"}, nil
	}

	err = uuc.repo.SetLockAmountToCardByAddress(ctx, user.Address)
	if nil != err {
		return &pb.AmountToCardReply{Status: "锁定失败"}, nil
	}

	if req.SendBody.Amount > uint64(user.Amount) {
		return &pb.AmountToCardReply{Status: "账号余额不足"}, nil
	}

	//if 100 > req.SendBody.Amount {
	//	return &pb.AmountToCardReply{Status: "划转最少100u"}, nil
	//}

	if 20 > req.SendBody.Amount {
		return &pb.AmountToCardReply{Status: "划转最少20u"}, nil
	}

	amountFloatSubFee := float64(req.SendBody.Amount) - float64(req.SendBody.Amount)*amountToRate
	if 0 >= amountFloatSubFee {
		return &pb.AmountToCardReply{Status: "手续费错误"}, nil
	}

	if 1 == req.SendBody.ToType {
		if 2 != user.CardTwo {
			return &pb.AmountToCardReply{Status: "无卡片记录，请先开通实体卡"}, nil
		}

		if 10 > len(user.CardTwoNumber) {
			return &pb.AmountToCardReply{Status: "无卡片记录，请先开通实体卡"}, nil
		}

		tmpRewardId := uint64(0)
		tmpOrderId := fmt.Sprintf("in-%d", time.Now().UnixNano())
		if err = uuc.tx.ExecTx(ctx, func(ctx context.Context) error { // 事务
			tmpRewardId, err = uuc.repo.AmountToCard(ctx, userId, float64(req.SendBody.Amount), amountFloatSubFee, 0)
			if nil != err {
				return err
			}

			err = uuc.repo.AmountToCardReward(ctx, userId, float64(req.SendBody.Amount), tmpOrderId, tmpRewardId, 1)
			if nil != err {
				return err
			}

			return nil
		}); nil != err {
			fmt.Println(err, "划转写入mysql错误", user)
			return &pb.AmountToCardReply{
				Status: "划转错误，联系管理员",
			}, nil
		}

		// 划转
		data, errTwo := InterlaceCardTransferIn(ctx, &InterlaceCardTransferInReq{
			AccountId:           interlaceAccountId,
			CardId:              user.CardTwoNumber,
			ClientTransactionId: tmpOrderId,
			Amount:              fmt.Sprintf("%.2f", amountFloatSubFee), // 字符串
		})
		if errTwo != nil {
			fmt.Println("InterlaceCardTransferIn error:", errTwo, data)
			return &pb.AmountToCardReply{
				Status: "划转错误，联系管理员，记录失败",
			}, nil
		}

	} else {
		if "success" != user.CardOrderId {
			return &pb.AmountToCardReply{Status: "无卡片记录，请先开通虚拟卡"}, nil
		}

		if 10 > len(user.CardNumber) {
			return &pb.AmountToCardReply{Status: "无卡片记录，请先开通实体卡"}, nil
		}

		tmpRewardId := uint64(0)
		tmpOrderId := fmt.Sprintf("in-%d", time.Now().UnixNano())
		if err = uuc.tx.ExecTx(ctx, func(ctx context.Context) error { // 事务
			tmpRewardId, err = uuc.repo.AmountToCard(ctx, userId, float64(req.SendBody.Amount), amountFloatSubFee, 0)
			if nil != err {
				return err
			}

			err = uuc.repo.AmountToCardReward(ctx, userId, float64(req.SendBody.Amount), tmpOrderId, tmpRewardId, 0)
			if nil != err {
				return err
			}

			return nil
		}); nil != err {
			fmt.Println(err, "划转写入mysql错误", user)
			return &pb.AmountToCardReply{
				Status: "划转错误，联系管理员",
			}, nil
		}

		// 划转
		data, errTwo := InterlaceCardTransferIn(ctx, &InterlaceCardTransferInReq{
			AccountId:           interlaceAccountId,
			CardId:              user.CardNumber,
			ClientTransactionId: tmpOrderId,
			Amount:              fmt.Sprintf("%.2f", amountFloatSubFee), // 字符串
		})
		if errTwo != nil {
			fmt.Println("InterlaceCardTransferIn error:", errTwo, data)
			return &pb.AmountToCardReply{
				Status: "划转错误，联系管理员，记录失败",
			}, nil
		}
	}

	return &pb.AmountToCardReply{
		Status: "ok",
	}, nil
}

func (uuc *UserUseCase) ChangePin(ctx context.Context, req *pb.ChangePinRequest, userId uint64) (*pb.ChangePinReply, error) {
	var (
		user *User
		err  error
	)
	user, err = uuc.repo.GetUserById(userId)
	if nil == user || nil != err {
		return &pb.ChangePinReply{Status: "用户不存在"}, nil
	}

	// 冻结
	if 1 == req.SendBody.CardType {
		res, errTwo := InterlaceSetCardPin(ctx, user.CardTwoNumber, &InterlaceSetCardPinReq{
			Pin:       req.SendBody.Pin,
			AccountId: interlaceAccountId,
		})
		if !res || errTwo != nil {
			return &pb.ChangePinReply{Status: "实体卡修改pin失败"}, nil
		}

	} else {
		res, errTwo := InterlaceSetCardPin(ctx, user.CardNumber, &InterlaceSetCardPinReq{
			Pin:       req.SendBody.Pin,
			AccountId: interlaceAccountId,
		})
		if !res || errTwo != nil {
			return &pb.ChangePinReply{Status: "虚拟卡修改pin失败"}, nil
		}
	}

	return &pb.ChangePinReply{Status: "ok"}, nil
}

func (uuc *UserUseCase) LookCardNewTwo(ctx context.Context, req *pb.LookCardRequest, userId uint64) (*pb.LookCardReply, error) {
	var (
		user *User
		err  error
	)
	user, err = uuc.repo.GetUserById(userId)
	if nil == user || nil != err {
		return &pb.LookCardReply{Status: "用户不存在"}, nil
	}

	if 1 == req.SendBody.CardType {
		err = uuc.repo.UploadCardChange(ctx, user.ID)
		if err != nil {
			return &pb.LookCardReply{Status: "用户不存在"}, nil
		}

	} else {
		err = uuc.repo.UploadCardChangeTwo(ctx, user.ID)
		if err != nil {
			return &pb.LookCardReply{Status: "用户不存在"}, nil
		}
	}

	return &pb.LookCardReply{Status: "ok"}, nil
}

func (uuc *UserUseCase) LookCardNew(ctx context.Context, req *pb.LookCardRequest, userId uint64) (*pb.LookCardReply, error) {
	var (
		user *User
		err  error
	)
	user, err = uuc.repo.GetUserById(userId)
	if nil == user || nil != err {
		return &pb.LookCardReply{Status: "用户不存在"}, nil
	}

	// 冻结
	if 1 == req.SendBody.CardType {
		err = uuc.repo.UploadCardOneLock(ctx, user.ID)
		if err != nil {
			return &pb.LookCardReply{Status: "用户不存在"}, nil
		}

		card, err := InterlaceFreezeCard(ctx, interlaceAccountId, user.CardNumber)
		if err != nil {
			fmt.Println("freeze error:", err)
			return &pb.LookCardReply{Status: "冻结虚拟卡失败"}, nil
		}
		fmt.Println("freeze ok, status =", card.Status) // 期望 FROZEN
	} else {
		err = uuc.repo.UploadCardTwoLock(ctx, user.ID)
		if err != nil {
			return &pb.LookCardReply{Status: "用户不存在"}, nil
		}

		card, err := InterlaceFreezeCard(ctx, interlaceAccountId, user.CardTwoNumber)
		if err != nil {
			fmt.Println("freeze error:", err)
			return &pb.LookCardReply{Status: "冻结实体卡失败"}, nil
		}
		fmt.Println("freeze ok, status =", card.Status) // 期望 FROZEN
	}

	return &pb.LookCardReply{Status: "ok"}, nil
}

func (uuc *UserUseCase) LookCard(ctx context.Context, req *pb.LookCardRequest, userId uint64) (*pb.LookCardReply, error) {
	var (
		user        *User
		err         error
		accessToken string
		//carInfo *CardSensitiveResponse
	)
	user, err = uuc.repo.GetUserById(userId)
	if nil == user || nil != err {
		return &pb.LookCardReply{Status: "用户不存在"}, nil
	}

	if 1 == req.SendBody.CardType {
		if "success" != user.CardOrderId {
			return &pb.LookCardReply{Status: "未激活虚拟卡"}, nil
		}

		if 10 > len(user.CardNumber) {
			return &pb.LookCardReply{Status: "未激活虚拟卡"}, nil
		}

		accessToken, err = InterlaceGetCardPrivateAccessToken(ctx, interlaceAccountId, user.CardNumber)
		if 0 >= len(accessToken) || nil != err {
			fmt.Println(err)
			return &pb.LookCardReply{Status: "查询错误"}, nil
		}
	} else if 2 == req.SendBody.CardType {
		if 2 != user.CardTwo {
			return &pb.LookCardReply{Status: "未激活实体卡"}, nil
		}

		if 10 > len(user.CardTwoNumber) {
			return &pb.LookCardReply{Status: "未激活实体卡"}, nil
		}
		accessToken, err = InterlaceGetCardPrivateAccessToken(ctx, interlaceAccountId, user.CardTwoNumber)
		if 0 >= len(accessToken) || nil != err {
			fmt.Println(err)
			return &pb.LookCardReply{Status: "查询错误"}, nil
		}
	} else {
		return &pb.LookCardReply{Status: "查询参数错误"}, nil
	}

	return &pb.LookCardReply{
		Status:      "ok",
		AccessToken: accessToken,
	}, nil
}

func (uuc *UserUseCase) AmountTo(ctx context.Context, req *pb.AmountToRequest, userId uint64) (*pb.AmountToReply, error) {
	var (
		user   *User
		toUser *User
		err    error
	)
	user, err = uuc.repo.GetUserById(userId)
	if nil == user || nil != err {
		return &pb.AmountToReply{Status: "用户不存在"}, nil
	}

	if req.SendBody.Amount > uint64(user.Amount) {
		return &pb.AmountToReply{Status: "账号余额不足"}, nil
	}

	if 30 > len(req.SendBody.Address) || 60 < len(req.SendBody.Address) {
		return &pb.AmountToReply{Status: "账号参数格式不正确"}, nil
	}

	toUser, err = uuc.repo.GetUserByAddress(req.SendBody.Address)
	if nil == toUser || nil != err {
		return &pb.AmountToReply{Status: "目标用户不存在"}, nil
	}

	if err = uuc.tx.ExecTx(ctx, func(ctx context.Context) error { // 事务
		err = uuc.repo.AmountTo(ctx, userId, toUser.ID, toUser.Address, float64(req.SendBody.Amount))
		if nil != err {
			return err
		}

		return nil
	}); nil != err {
		fmt.Println(err, "划转写入mysql错误", user)
		return &pb.AmountToReply{
			Status: "划转错误，联系管理员",
		}, nil
	}

	return &pb.AmountToReply{
		Status: "ok",
	}, nil
}

func (uuc *UserUseCase) CodeList(ctx context.Context, req *pb.CodeListRequest, userId uint64) (*pb.CodeListReply, error) {
	res := make([]*pb.CodeListReply_List, 0)

	var (
		user     *User
		err      error
		count    int64
		cardNum  string
		codeList []*CardOrder
	)
	user, err = uuc.repo.GetUserById(userId)
	if nil == user || nil != err {
		return &pb.CodeListReply{Status: "用户不存在"}, nil
	}

	if 2 == req.Num {
		if 5 < len(user.CardNumberRelTwo) {
			cardNum = MaskCard8_6_4(user.CardNumberRelTwo)
		}
	} else {
		if 5 < len(user.CardNumberRel) {
			cardNum = MaskCard8_6_4(user.CardNumberRel)
		}
	}

	if 5 >= len(cardNum) {
		return &pb.CodeListReply{
			Status: "ok",
			Count:  uint64(count),
			List:   res,
		}, err
	}

	codeList, err, count = uuc.repo.GetUserCodePage(ctx, &Pagination{
		PageNum:  int(req.Page),
		PageSize: 20,
	}, cardNum)
	if nil != err {
		return &pb.CodeListReply{
			Status: "ok",
			Count:  uint64(count),
			List:   res,
		}, err
	}

	for _, v := range codeList {
		res = append(res, &pb.CodeListReply_List{
			CreatedAt: v.Time.Add(8 * time.Hour).Format("2006-01-02 15:04:05"),
			Code:      v.Code,
		})
	}

	return &pb.CodeListReply{List: res, Count: uint64(count), Status: "ok"}, nil
}

func MaskCard8_6_4(card string) string {
	// 只保留数字
	d := onlyDigits(card)
	if len(d) < 12 { // 太短就原样返回（你也可以返回空）
		return card
	}

	// 前8位
	prefix := d
	if len(d) >= 8 {
		prefix = d[:8]
	}

	// 后4位
	suffix := d
	if len(d) >= 4 {
		suffix = d[len(d)-4:]
	}

	return prefix + "xxxxxx" + suffix
}

// onlyDigits 仅保留数字字符
func onlyDigits(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func (uuc *UserUseCase) Withdraw(ctx context.Context, req *pb.WithdrawRequest, userId uint64) (*pb.WithdrawReply, error) {
	var (
		user         *User
		err          error
		configs      []*Config
		withdrawRate float64
	)

	// 配置
	configs, err = uuc.repo.GetConfigByKeys("withdraw_rate")
	if nil != configs {
		for _, vConfig := range configs {
			if "withdraw_rate" == vConfig.KeyName {
				withdrawRate, _ = strconv.ParseFloat(vConfig.Value, 10)
			}
		}
	}

	user, err = uuc.repo.GetUserById(userId)
	if nil == user || nil != err {
		return &pb.WithdrawReply{Status: "用户不存在"}, nil
	}

	if req.SendBody.Amount > uint64(user.Amount) {
		return &pb.WithdrawReply{Status: "账号余额不足"}, nil
	}

	amountFloatSubFee := float64(req.SendBody.Amount) - float64(req.SendBody.Amount)*withdrawRate
	if 0 >= amountFloatSubFee {
		return &pb.WithdrawReply{Status: "手续费错误"}, nil
	}

	if err = uuc.tx.ExecTx(ctx, func(ctx context.Context) error { // 事务
		err = uuc.repo.Withdraw(ctx, userId, float64(req.SendBody.Amount), amountFloatSubFee, user.Address)
		if nil != err {
			return err
		}

		return nil
	}); nil != err {
		return &pb.WithdrawReply{
			Status: "提现错误，联系管理员",
		}, nil
	}

	return &pb.WithdrawReply{
		Status: "ok",
	}, nil
}

func (uuc *UserUseCase) Upload(ctx transporthttp.Context) (err error) {

	return nil

	w := ctx.Response() // http.ResponseWriter
	r := ctx.Request()  // *http.Request

	// 拆分：文件上限 vs 请求体上限（请求体要比文件略大，留给 multipart/字段开销）
	const maxFile int64 = 10 << 20    // 10MB 文件
	const maxBody = maxFile + 512<<10 // 10MB + 512KB（如果表单字段多可再加大点）

	// 1) 限制整个请求体
	r.Body = http.MaxBytesReader(w, r.Body, maxBody)

	// 2) 限制 multipart 解析时使用的内存（超出会落到临时文件；但请求体仍受 maxBody 限制）
	if err = r.ParseMultipartForm(1 << 20); err != nil {
		return err // 常见：http: request body too large
	}

	name := ctx.Request().FormValue("address")
	num := ctx.Request().FormValue("num")

	var user *User
	user, err = uuc.repo.GetUserByAddress(name)
	if user == nil || err != nil {
		return
	}

	file, header, err := ctx.Request().FormFile("file")
	if err != nil {
		return
	}
	defer file.Close()

	// 3) 快速拦截（header.Size 不完全可信，但能提前挡掉大部分）
	if header != nil && header.Size > maxFile {
		return nil
	}

	uS := strconv.FormatUint(user.ID, 10)
	picName := uS + num + ".png"

	if "one" == num {
		if "no" == user.Pic {
			if err = uuc.repo.UploadCardPic(ctx, user.ID, picName); err != nil {
				return err
			}
		}
	} else {
		if "no" == user.PicTwo {
			if err = uuc.repo.UploadCardPicTwo(ctx, user.ID, picName); err != nil {
				return err
			}
		}
	}

	dstPath := "/www/wwwroot/www.royalpay.tv/images/" + picName

	tmpPath := dstPath + ".tmp"

	// 先写临时文件，成功后 rename，避免半截文件
	imageFile, err := os.Create(tmpPath)
	if err != nil {
		return err
	}

	// 4) 关键：最多读 maxFile+1 字节，超过就能判断超限
	n, copyErr := io.CopyN(imageFile, file, maxFile+1)

	closeErr := imageFile.Close()
	if copyErr != nil && copyErr != io.EOF {
		_ = os.Remove(tmpPath)
		return copyErr
	}
	if closeErr != nil {
		_ = os.Remove(tmpPath)
		return closeErr
	}
	if n > maxFile {
		_ = os.Remove(tmpPath)
		return nil
	}

	// 原子替换到最终路径
	if err := os.Rename(tmpPath, dstPath); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}

	return nil
}

type CreateCardResponse struct {
	CardID      string `json:"cardId"`
	CardOrderID string `json:"cardOrderId"`
	CreateTime  string `json:"createTime"`
	CardStatus  string `json:"cardStatus"`
	OrderStatus string `json:"orderStatus"`
}

func GenerateSign(params map[string]interface{}, signKey string) string {
	// 1. 排除 sign 字段
	var keys []string
	for k := range params {
		if k != "sign" {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	// 2. 拼接 key + value 字符串
	var sb strings.Builder
	sb.WriteString(signKey)

	for _, k := range keys {
		sb.WriteString(k)
		value := params[k]

		var strValue string
		switch v := value.(type) {
		case string:
			strValue = v
		case float64, int, int64, bool:
			strValue = fmt.Sprintf("%v", v)
		default:
			// map、slice 等复杂类型用 JSON 编码
			jsonBytes, err := json.Marshal(v)
			if err != nil {
				strValue = ""
			} else {
				strValue = string(jsonBytes)
			}
		}
		sb.WriteString(strValue)
	}

	signString := sb.String()
	//fmt.Println("md5前字符串", signString)

	// 3. 进行 MD5 加密
	hash := md5.Sum([]byte(signString))
	return hex.EncodeToString(hash[:])
}

func CreateCardRequestWithSign() (*CreateCardResponse, error) {
	//url := "https://test-api.ispay.com/dev-api/vcc/api/v1/cards/create"
	//url := "https://www.ispay.com/prod-api/vcc/api/v1/cards/create"
	url := "http://120.79.173.55:9102/prod-api/vcc/api/v1/cards/create"

	reqBody := map[string]interface{}{
		"merchantId":    "322338",
		"cardCurrency":  "USD",
		"cardAmount":    1000000,
		"cardholderId":  10001,
		"cardProductId": 20001,
		"cardSpendRule": map[string]interface{}{
			"dailyLimit":   250000,
			"monthlyLimit": 1000000,
		},
		"cardRiskControl": map[string]interface{}{
			"allowedMerchants": []string{"ONLINE"},
			"blockedCountries": []string{},
		},
	}

	sign := GenerateSign(reqBody, "j4gqNRcpTDJr50AP2xd9obKWZIKWbeo9")
	// 请求体（包括嵌套结构）
	reqBody["sign"] = sign

	jsonData, _ := json.Marshal(reqBody)
	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	req.Header.Set("Content-Type", "application/json")

	//fmt.Println("请求报文:", string(jsonData))

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func(Body io.ReadCloser) {
		errTwo := Body.Close()
		if errTwo != nil {

		}
	}(resp.Body)

	body, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, err
	}

	fmt.Println("响应报文:", string(body)) // ← 打印响应内容

	var result *CreateCardResponse
	if err = json.Unmarshal(body, result); err != nil {
		return nil, err
	}

	return result, nil
}

type CreateCardholderResponse struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
	Data struct {
		HolderID    string `json:"holderId"`
		Email       string `json:"email"`
		FirstName   string `json:"firstName"`
		LastName    string `json:"lastName"`
		BirthDate   string `json:"birthDate"`
		CountryCode string `json:"countryCode"`
		PhoneNumber string `json:"phoneNumber"`

		DeliveryAddress DeliveryAddress `json:"deliveryAddress"`
		//ProofFile       ProofFile       `json:"proofFile"`
	} `json:"data"`
}

type DeliveryAddress struct {
	City    string `json:"city"`
	Country string `json:"country"`
	Street  string `json:"street"`
}

type ProofFile struct {
	FileBase64 string `json:"fileBase64"`
	FileType   string `json:"fileType"`
}

func CreateCardholderRequest(productId uint64, user *User) (*CreateCardholderResponse, error) {
	//baseURL := "https://www.ispay.com/prod-api/vcc/api/v1/cards/holders/create"
	baseURL := "http://120.79.173.55:9102/prod-api/vcc/api/v1/cards/holders/create"

	reqBody := map[string]interface{}{
		"productId":   productId,
		"merchantId":  "322338",
		"email":       user.Email,
		"firstName":   user.FirstName,
		"lastName":    user.LastName,
		"birthDate":   user.BirthDate,
		"countryCode": user.CountryCode,
		"phoneNumber": user.Phone,
		"deliveryAddress": map[string]interface{}{
			"city":       user.City,
			"country":    user.CountryCode,
			"street":     user.Street,
			"postalCode": user.PostalCode,
		},
	}

	// 生成签名
	sign := GenerateSign(reqBody, "j4gqNRcpTDJr50AP2xd9obKWZIKWbeo9") // 用你的密钥替换
	reqBody["sign"] = sign

	// 构造请求
	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("json marshal error: %v", err)
	}

	req, err := http.NewRequest("POST", baseURL, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("new request error: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Language", "zh_CN")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http do error: %v", err)
	}
	defer func(Body io.ReadCloser) {
		errTwo := Body.Close()
		if errTwo != nil {

		}
	}(resp.Body)

	body, _ := io.ReadAll(resp.Body)
	fmt.Println("响应报文:", string(body))

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http status not ok: %v", resp.StatusCode)
	}

	var result CreateCardholderResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("json unmarshal error: %v", err)
	}

	return &result, nil
}

func UpdateCardholderRequest(productId uint64, user *User) (*CreateCardholderResponse, error) {
	//baseURL := "https://www.ispay.com/prod-api/vcc/api/v1/cards/holders/create"
	baseURL := "http://120.79.173.55:9102/prod-api/vcc/api/v1/cards/holders/update"

	reqBody := map[string]interface{}{
		"holderId":    user.CardUserId,
		"productId":   productId,
		"merchantId":  "322338",
		"email":       user.Email,
		"firstName":   user.FirstName,
		"lastName":    user.LastName,
		"birthDate":   user.BirthDate,
		"countryCode": user.CountryCode,
		"phoneNumber": user.Phone,
		"deliveryAddress": map[string]interface{}{
			"city":       user.City,
			"country":    user.CountryCode,
			"street":     user.Street,
			"postalCode": user.PostalCode,
		},
	}

	// 生成签名
	sign := GenerateSign(reqBody, "j4gqNRcpTDJr50AP2xd9obKWZIKWbeo9") // 用你的密钥替换
	reqBody["sign"] = sign

	// 构造请求
	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("json marshal error: %v", err)
	}

	req, err := http.NewRequest("POST", baseURL, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("new request error: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Language", "zh_CN")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http do error: %v", err)
	}
	defer func(Body io.ReadCloser) {
		errTwo := Body.Close()
		if errTwo != nil {

		}
	}(resp.Body)

	body, _ := io.ReadAll(resp.Body)
	fmt.Println("响应报文:", string(body))

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http status not ok: %v", resp.StatusCode)
	}

	var result CreateCardholderResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("json unmarshal error: %v", err)
	}

	return &result, nil
}

type CardProductListResponse struct {
	Total int           `json:"total"`
	Rows  []CardProduct `json:"rows"`
	Code  int           `json:"code"`
	Msg   string        `json:"msg"`
}

type CardProduct struct {
	ProductId          string       `json:"productId"` // ← 改成 string
	ProductName        string       `json:"productName"`
	ModeType           string       `json:"modeType"`
	CardBin            string       `json:"cardBin"`
	CardForm           []string     `json:"cardForm"`
	MaxCardQuota       uint64       `json:"maxCardQuota"`
	CardScheme         string       `json:"cardScheme"`
	NoPinPaymentAmount []AmountItem `json:"noPinPaymentAmount"`
	CardCurrency       []string     `json:"cardCurrency"`
	CreateTime         string       `json:"createTime"`
	UpdateTime         string       `json:"updateTime"`
	ProductStatus      string       `json:"productStatus"`
}

type AmountItem struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

func GetCardProducts() (*CardProductListResponse, error) {
	baseURL := "http://120.79.173.55:9102/prod-api/vcc/api/v1/cards/products/all"

	reqBody := map[string]interface{}{
		"merchantId": "322338",
	}

	sign := GenerateSign(reqBody, "j4gqNRcpTDJr50AP2xd9obKWZIKWbeo9")

	params := url.Values{}
	params.Set("merchantId", "322338")
	params.Set("sign", sign)

	fullURL := fmt.Sprintf("%s?%s", baseURL, params.Encode())

	req, err := http.NewRequest("GET", fullURL, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Language", "zh_CN")

	client := &http.Client{
		Timeout: 10 * time.Second,
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func(Body io.ReadCloser) {
		errTwo := Body.Close()
		if errTwo != nil {

		}
	}(resp.Body)

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	//fmt.Println("响应报文:", string(body))

	var result CardProductListResponse
	err = json.Unmarshal(body, &result)
	if err != nil {
		fmt.Println("JSON 解析失败:", err)
		return nil, err
	}

	//fmt.Println(result)

	return &result, nil
}

type CardSensitiveResponse struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
	Data struct {
		Pan    string `json:"pan"`
		Pin    string `json:"pin"`
		CVV    string `json:"cvv"`
		Expire string `json:"expire"`
	} `json:"data"`
}

func GetCardSensitiveInfo(cardId string) (*CardSensitiveResponse, error) {
	//baseUrl := "https://www.ispay.com/prod-api/vcc/api/v1/cards/sensitive"
	baseUrl := "http://120.79.173.55:9102/prod-api/vcc/api/v1/cards/sensitive"

	reqBody := map[string]interface{}{
		"merchantId": "322338",
		"cardId":     cardId,
	}

	sign := GenerateSign(reqBody, "j4gqNRcpTDJr50AP2xd9obKWZIKWbeo9")
	reqBody["sign"] = sign

	jsonData, _ := json.Marshal(reqBody)
	req, _ := http.NewRequest("POST", baseUrl, bytes.NewBuffer(jsonData))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Language", "zh_CN")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func(Body io.ReadCloser) {
		errTwo := Body.Close()
		if errTwo != nil {

		}
	}(resp.Body)

	body, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP请求失败: %s", string(body))
	}

	//fmt.Println("响应报文:", string(body))

	var result CardSensitiveResponse
	if err = json.Unmarshal(body, &result); err != nil {
		fmt.Println("敏感信息 JSON 解析失败:", err)
		return nil, err
	}

	return &result, nil
}

type CardRechargeResponse struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
	Data struct {
		CardID       string `json:"cardId"`
		CardOrderID  string `json:"cardOrderId"`
		OrderType    string `json:"orderType"`
		CardCurrency string `json:"cardCurrency"`
		CreateTime   string `json:"createTime"`
		UpdateTime   string `json:"updateTime"`
		CompleteTime string `json:"completeTime"`
		OrderStatus  string `json:"orderStatus"`
	} `json:"data"`
}

func RechargeCard(cardId string, rechargeAmount uint64) (*CardRechargeResponse, error) {
	//baseUrl := "https://www.ispay.com/prod-api/vcc/api/v1/cards/recharge"
	baseUrl := "http://120.79.173.55:9102/prod-api/vcc/api/v1/cards/recharge"

	reqBody := map[string]interface{}{
		"merchantId":     "322338",
		"cardId":         cardId,
		"rechargeAmount": rechargeAmount,
	}

	sign := GenerateSign(reqBody, "j4gqNRcpTDJr50AP2xd9obKWZIKWbeo9")
	reqBody["sign"] = sign

	jsonData, _ := json.Marshal(reqBody)
	req, _ := http.NewRequest("POST", baseUrl, bytes.NewBuffer(jsonData))
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP请求失败: %s", string(body))
	}

	fmt.Println("响应报文:", string(body))

	var result CardRechargeResponse
	if err = json.Unmarshal(body, &result); err != nil {
		fmt.Println("充值响应解析失败:", err)
		return nil, err
	}

	return &result, nil
}

type CardTransactionListResponse struct {
	Code  uint64                  `json:"code"`  // 接口状态码
	Msg   string                  `json:"msg"`   // 返回消息
	Total uint64                  `json:"total"` // 总条数
	Rows  []CardTransactionRecord `json:"rows"`  // 交易列表
}

type CardTransactionRecord struct {
	ID                      string                 `json:"id"`
	Pan                     string                 `json:"pan"`
	TradeNo                 string                 `json:"tradeNo"`
	Type                    string                 `json:"type"`
	Status                  string                 `json:"status"`
	TradeAmount             string                 `json:"tradeAmount"`
	TradeCurrency           string                 `json:"tradeCurrency"`
	Timestamp               string                 `json:"timestamp"`
	ServiceFee              string                 `json:"serviceFee"`
	ActualTransactionAmount string                 `json:"actualTransactionAmount"`
	CurrentBalance          string                 `json:"currentBalance"`
	CreateTime              string                 `json:"createTime"`
	TradeDescription        string                 `json:"tradeDescription"`
	MerchantData            map[string]interface{} `json:"merchantData"` // 用 map 保证兼容性
}

func GetCardTransactionList(cardId, pageNum, pageSize uint64) (*CardTransactionListResponse, error) {
	baseUrl := "http://120.79.173.55:9102/prod-api/vcc/api/v1/cards/transactions/list"

	// 1. 构造参数（全部为一级扁平字段）
	reqParams := map[string]interface{}{
		"merchantId":    "322338",
		"cardId":        cardId,
		"pageSize":      pageSize,
		"pageNum":       pageNum,
		"orderByColumn": "createTime",
		"isAsc":         "desc",
	}

	// 2. 生成签名（假设你有此函数）
	sign := GenerateSign(reqParams, "j4gqNRcpTDJr50AP2xd9obKWZIKWbeo9")
	reqParams["sign"] = sign

	// 3. 构造 query string
	query := url.Values{}
	for k, v := range reqParams {
		query.Set(k, fmt.Sprintf("%v", v))
	}

	fullUrl := baseUrl + "?" + query.Encode()
	//fmt.Println("请求 URL:", fullUrl)

	// 4. 发起 GET 请求
	req, err := http.NewRequest("GET", fullUrl, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Language", "zh_CN")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func(Body io.ReadCloser) {
		errTwo := Body.Close()
		if errTwo != nil {

		}
	}(resp.Body)

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return nil, err
	}

	//fmt.Println("响应报文:", string(body))

	// 5. 解析响应
	var result CardTransactionListResponse
	if err = json.Unmarshal(body, &result); err != nil {
		fmt.Println("JSON 解析失败:", err)
		return nil, err
	}

	return &result, nil
}

type CardInfoResponse struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
	Data struct {
		CardID     string `json:"cardId"`
		Pan        string `json:"pan"`
		CardStatus string `json:"cardStatus"`
		Balance    string `json:"balance"`
		Holder     struct {
			HolderID string `json:"holderId"`
		} `json:"holder"`
	} `json:"data"`
}

func GetCardInfoRequestWithSign(cardId string) (*CardInfoResponse, error) {
	baseUrl := "http://120.79.173.55:9102/prod-api/vcc/api/v1/cards/info"
	//baseUrl := "https://www.ispay.com/prod-api/vcc/api/v1/cards/info"

	reqBody := map[string]interface{}{
		"merchantId": "322338",
		"cardId":     cardId, // 如果需要传 cardId，根据实际接口文档添加
	}

	sign := GenerateSign(reqBody, "j4gqNRcpTDJr50AP2xd9obKWZIKWbeo9")
	reqBody["sign"] = sign

	jsonData, _ := json.Marshal(reqBody)
	req, _ := http.NewRequest("POST", baseUrl, bytes.NewBuffer(jsonData))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Language", "zh_CN")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func(Body io.ReadCloser) {
		errTwo := Body.Close()
		if errTwo != nil {

		}
	}(resp.Body)

	body, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("request failed: %s", string(body))
	}

	//fmt.Println("响应报文:", string(body))

	var result CardInfoResponse
	if err = json.Unmarshal(body, &result); err != nil {
		fmt.Println("卡信息 JSON 解析失败:", err)
		return nil, err
	}

	return &result, nil
}

// ================= Interlace 授权配置 & 缓存 =================

const (
	interlaceBaseURL      = "https://api-sandbox.interlace.money/open-api/v3"
	interlaceClientID     = "interlacedc0330757f216112"
	interlaceClientSecret = "c0d8019217ad4903bf09336320a4ddd9" // v3 的接口目前用不到 secret，但建议以后放到配置/环境变量
	interlaceAccountId    = "571795"
)

// 缓存在当前进程里，如果你将来多实例部署/重启频繁，可以再扩展成 Redis 存储
type interlaceAuthCache struct {
	AccessToken  string
	RefreshToken string
	ExpireAt     int64 // unix 秒，提前留一点余量
}

var (
	interlaceAuth    = &interlaceAuthCache{}
	interlaceAuthMux sync.Mutex
)

// GetInterlaceAccessToken 获取一个当前可用的 accessToken
// 1. 如果缓存里有且没过期，直接返回
// 2. 否则调用 GetCode + Generate Access Token 重新获取
func GetInterlaceAccessToken(ctx context.Context) (string, error) {
	interlaceAuthMux.Lock()
	defer interlaceAuthMux.Unlock()

	now := time.Now().Unix()
	// 缓存未过期，直接用（提前 60 秒过期，避免边界）
	if 0 >= len(interlaceAuth.AccessToken) && now < interlaceAuth.ExpireAt-60 {
		return interlaceAuth.AccessToken, nil
	}

	// 这里可以先尝试用 refreshToken 刷新（如果你想用 refresh-token 接口）
	// 为了简单稳定，这里直接重新 Get Code + Access Token
	code, err := interlaceGetCode(ctx)
	if err != nil {
		return "", fmt.Errorf("get interlace code failed: %w", err)
	}

	accessToken, refreshToken, expiresIn, t, err := interlaceGenerateAccessToken(ctx, code)
	if err != nil {
		return "", fmt.Errorf("generate interlace access token failed: %w", err)
	}

	if 0 >= len(accessToken) {
		return "", nil
	}

	interlaceAuth.AccessToken = accessToken
	interlaceAuth.RefreshToken = refreshToken
	interlaceAuth.ExpireAt = t + expiresIn

	return accessToken, nil
}

// Get a code 响应结构
type interlaceGetCodeResp struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Data    struct {
		Timestamp int64  `json:"timestamp"`
		Code      string `json:"code"`
	} `json:"data"`
}

func interlaceGetCode(ctx context.Context) (string, error) {
	urlStr := fmt.Sprintf("%s/oauth/authorize?clientId=%s", interlaceBaseURL, interlaceClientID)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, urlStr, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Accept", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer func(Body io.ReadCloser) {
		_ = Body.Close()
	}(resp.Body)

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("interlace get code http %d: %s", resp.StatusCode, string(body))
	}

	var result interlaceGetCodeResp
	if err := json.Unmarshal(body, &result); err != nil {
		return "", fmt.Errorf("interlace get code unmarshal: %w", err)
	}

	if result.Code != "000000" {
		return "", fmt.Errorf("interlace get code failed: code=%s msg=%s", result.Code, result.Message)
	}
	if result.Data.Code == "" {
		return "", fmt.Errorf("interlace get code success but orderId empty")
	}

	return result.Data.Code, nil
}

// Generate an access token 响应结构
type interlaceAccessTokenResp struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Data    struct {
		AccessToken  string `json:"accessToken"`
		RefreshToken string `json:"refreshToken"`
		ExpiresIn    int64  `json:"expiresIn"` // 有效期秒数，比如 86400
		Timestamp    int64  `json:"timestamp"`
	} `json:"data"`
}

func interlaceGenerateAccessToken(ctx context.Context, code string) (accessToken, refreshToken string, expiresIn, t int64, err error) {
	urlStr := fmt.Sprintf("%s/oauth/access-token", interlaceBaseURL)

	reqBody := map[string]interface{}{
		"clientId": interlaceClientID,
		"code":     code,
	}
	jsonData, _ := json.Marshal(reqBody)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, urlStr, bytes.NewReader(jsonData))
	if err != nil {
		return "", "", 0, 0, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", "", 0, 0, err
	}
	defer func(Body io.ReadCloser) {
		_ = Body.Close()
	}(resp.Body)

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", "", 0, 0, err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", "", 0, 0, fmt.Errorf("interlace access-token http %d: %s", resp.StatusCode, string(body))
	}

	var result interlaceAccessTokenResp
	if err := json.Unmarshal(body, &result); err != nil {
		return "", "", 0, 0, fmt.Errorf("interlace access-token unmarshal: %w", err)
	}

	if result.Code != "000000" {
		return "", "", 0, 0, fmt.Errorf("interlace access-token failed: code=%s msg=%s", result.Code, result.Message)
	}
	if result.Data.AccessToken == "" {
		return "", "", 0, 0, fmt.Errorf("interlace access-token success but accessToken empty")
	}

	return result.Data.AccessToken, result.Data.RefreshToken, result.Data.ExpiresIn, result.Data.Timestamp, nil
}

type InterlaceCardPrivateTokenResp struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Data    struct {
		AccessToken string `json:"accessToken"`
	} `json:"data"`
}

// InterlaceGetCardPrivateAccessToken 获取某张卡的 iframe 用一次性 accessToken
func InterlaceGetCardPrivateAccessToken(ctx context.Context, accountId, cardId string) (string, error) {
	if accountId == "" {
		return "", fmt.Errorf("accountId is required")
	}
	if cardId == "" {
		return "", fmt.Errorf("cardId is required")
	}

	accessToken, err := GetInterlaceAccessToken(ctx)
	if err != nil || accessToken == "" {
		fmt.Println("获取access token错误")
		return "", err
	}

	//fmt.Println(accessToken)
	base := interlaceBaseURL + "/cards/" + cardId + "/private-info/access-token"

	//fmt.Println(base, accessToken)

	q := url.Values{}
	q.Set("accountId", accountId)
	urlStr := base + "?" + q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, urlStr, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("x-access-token", accessToken)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	//fmt.Println(string(body))

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		fmt.Println("at", interlaceAuth, time.Now().Unix())
		return "", fmt.Errorf("interlace card private token http %d: %s", resp.StatusCode, string(body))
	}

	var outer InterlaceCardPrivateTokenResp
	if err := json.Unmarshal(body, &outer); err != nil {
		return "", fmt.Errorf("card private token unmarshal: %w", err)
	}
	if outer.Code != "000000" {
		return "", fmt.Errorf("card private token failed: code=%s msg=%s", outer.Code, outer.Message)
	}

	return outer.Data.AccessToken, nil
}

type InterlaceFeeDetail struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
	FeeType  string `json:"feeType"`
}

type InterlaceCardTransferData struct {
	ID                       string               `json:"id"`
	AccountId                string               `json:"accountId"`
	CardId                   string               `json:"cardId"`
	CardholderId             string               `json:"cardholderId"`
	CardTransactionId        string               `json:"cardTransactionId"`
	Currency                 string               `json:"currency"`
	Amount                   string               `json:"amount"`
	Fee                      string               `json:"fee"`
	FeeDetails               []InterlaceFeeDetail `json:"feeDetails"`
	ClientTransactionId      string               `json:"clientTransactionId"`
	RelatedCardTransactionId string               `json:"relatedCardTransactionId"`
	TransactionDisplayId     string               `json:"transactionDisplayId"`
	Type                     int32                `json:"type"`
	Status                   string               `json:"status"`

	MerchantName    string `json:"merchantName"`
	Mcc             string `json:"mcc"`
	MccCategory     string `json:"mccCategory"`
	MerchantCity    string `json:"merchantCity"`
	MerchantCountry string `json:"merchantCountry"`
	MerchantState   string `json:"merchantState"`
	MerchantZipcode string `json:"merchantZipcode"`
	MerchantMid     string `json:"merchantMid"`

	TransactionTime     string `json:"transactionTime"`
	TransactionCurrency string `json:"transactionCurrency"`
	TransactionAmount   string `json:"transactionAmount"`
	CreateTime          string `json:"createTime"`
	Remark              string `json:"remark"`
	Detail              string `json:"detail"`
}

type InterlaceCardTransferInReq struct {
	AccountId           string `json:"accountId"`           // 账户 UUID
	CardId              string `json:"cardId"`              // 卡 UUID
	ClientTransactionId string `json:"clientTransactionId"` // 自定义交易 ID
	Amount              string `json:"amount"`              // 划转金额（字符串）
}

// 外层响应
type InterlaceCardTransferInResp struct {
	Code    string                    `json:"code"`
	Message string                    `json:"message"`
	Data    InterlaceCardTransferData `json:"data"`
}

// InterlaceCardTransferIn 预付卡划转入（从 Quantum 账户到卡）
func InterlaceCardTransferIn(ctx context.Context, in *InterlaceCardTransferInReq) (*InterlaceCardTransferData, error) {
	if in == nil {
		return nil, fmt.Errorf("transfer in req is nil")
	}
	if in.AccountId == "" {
		return nil, fmt.Errorf("accountId is required")
	}
	if in.CardId == "" {
		return nil, fmt.Errorf("cardId is required")
	}
	if in.ClientTransactionId == "" {
		return nil, fmt.Errorf("clientTransactionId is required")
	}
	if in.Amount == "" {
		return nil, fmt.Errorf("amount is required")
	}

	accessToken, err := GetInterlaceAccessToken(ctx)
	if err != nil || accessToken == "" {
		fmt.Println("获取access token错误")
		return nil, err
	}

	// interlaceBaseURL 建议: https://api-sandbox.interlace.money/open-api/v3
	base := interlaceBaseURL + "/cards/transfer-in"

	bodyBytes, err := json.Marshal(in)
	if err != nil {
		return nil, fmt.Errorf("marshal transfer in body: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-access-token", accessToken)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	fmt.Println("transfer-in resp:", string(respBody))

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		fmt.Println("at", interlaceAuth, time.Now().Unix())
		return nil, fmt.Errorf("interlace transfer in http %d: %s", resp.StatusCode, string(respBody))
	}

	var outer InterlaceCardTransferInResp
	if err := json.Unmarshal(respBody, &outer); err != nil {
		return nil, fmt.Errorf("transfer in unmarshal: %w", err)
	}
	if outer.Code != "000000" {
		return nil, fmt.Errorf("transfer in failed: code=%s msg=%s", outer.Code, outer.Message)
	}

	return &outer.Data, nil
}

// /cards/{id}/card-summary 返回体
type InterlaceCardSummaryResp struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Data    struct {
		CardId    string `json:"cardId"`
		AccountId string `json:"accountId"`

		Balance struct {
			ID        string `json:"id"`
			Available string `json:"available"`
			Currency  string `json:"currency"`
		} `json:"balance"`

		Statistics struct {
			Consumption    string `json:"consumption"`
			Reversal       string `json:"reversal"`
			ReversalFee    string `json:"reversalFee"`
			Refund         string `json:"refund"`
			RefundFee      string `json:"refundFee"`
			NetConsumption string `json:"netConsumption"`
			Currency       string `json:"currency"`
		} `json:"statistics"`

		VelocityControl struct {
			Type      string `json:"type"` // DAY/WEEK/MONTH/.../NA
			Limit     string `json:"limit"`
			Available string `json:"available"`
		} `json:"velocityControl"`
	} `json:"data"`
}

// InterlaceGetCardSummary 获取卡片 summary（余额/统计/限额）
func InterlaceGetCardSummary(ctx context.Context, accountId, cardId string) (*InterlaceCardSummaryResp, error) {
	if accountId == "" {
		return nil, fmt.Errorf("accountId is required")
	}
	if cardId == "" {
		return nil, fmt.Errorf("cardId is required")
	}

	accessToken, err := GetInterlaceAccessToken(ctx)
	if err != nil || accessToken == "" {
		fmt.Println("获取access token错误")
		return nil, err
	}

	// interlaceBaseURL 建议: https://api-sandbox.interlace.money/open-api/v3
	base := interlaceBaseURL + "/cards/" + cardId + "/card-summary"

	q := url.Values{}
	q.Set("accountId", accountId)
	urlStr := base + "?" + q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, urlStr, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("x-access-token", accessToken)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	// 方便你调试
	// fmt.Println("card-summary resp:", string(body))

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {

		fmt.Println("at", interlaceAuth, time.Now().Unix())
		return nil, fmt.Errorf("interlace card summary http %d: %s", resp.StatusCode, string(body))
	}

	var outer InterlaceCardSummaryResp
	if err := json.Unmarshal(body, &outer); err != nil {
		return nil, fmt.Errorf("card summary unmarshal: %w", err)
	}
	if outer.Code != "000000" {
		return nil, fmt.Errorf("card summary failed: code=%s msg=%s", outer.Code, outer.Message)
	}

	return &outer, nil
}

type InterlaceTxnListReq struct {
	AccountId string // 必填

	ID                  string // transaction id
	ClientTransactionId string
	CardId              string
	Type                string // "0".."14"
	Status              string // CLOSED/PENDING/FAIL

	StartTime string // timestamp（按文档写 string，直接透传）
	EndTime   string // timestamp

	Limit int // 1-100 默认 10
	Page  int // >=1 默认 1
}

type InterlaceTransaction struct {
	ID                string `json:"id"`
	AccountId         string `json:"accountId"`
	CardId            string `json:"cardId"`
	CardholderId      string `json:"cardholderId"`
	CardTransactionId string `json:"cardTransactionId"`

	Currency string `json:"currency"`
	Amount   string `json:"amount"`
	Fee      string `json:"fee"`

	FeeDetails []InterlaceFeeDetail `json:"feeDetails"`

	ClientTransactionId      string `json:"clientTransactionId"`
	RelatedCardTransactionId string `json:"relatedCardTransactionId"`
	TransactionDisplayId     string `json:"transactionDisplayId"`

	Type   int32  `json:"type"`
	Status string `json:"status"`

	MerchantName    string `json:"merchantName"`
	Mcc             string `json:"mcc"`
	MccCategory     string `json:"mccCategory"`
	MerchantCity    string `json:"merchantCity"`
	MerchantCountry string `json:"merchantCountry"`
	MerchantState   string `json:"merchantState"`
	MerchantZipcode string `json:"merchantZipcode"`
	MerchantMid     string `json:"merchantMid"`

	TransactionTime     string `json:"transactionTime"`
	TransactionCurrency string `json:"transactionCurrency"`
	TransactionAmount   string `json:"transactionAmount"`
	CreateTime          string `json:"createTime"`

	Remark string `json:"remark"`
	Detail string `json:"detail"`
}

type InterlaceTxnListResp struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Data    struct {
		List  []InterlaceTransaction `json:"list"`
		Total string                 `json:"total"`
	} `json:"data"`
}

// InterlaceListTransactions 拉交易流水
func InterlaceListTransactions(ctx context.Context, in *InterlaceTxnListReq) ([]*InterlaceTransaction, string, error) {
	if in == nil {
		return nil, "", fmt.Errorf("txn list req is nil")
	}
	if in.AccountId == "" {
		return nil, "", fmt.Errorf("accountId is required")
	}

	accessToken, err := GetInterlaceAccessToken(ctx)
	if err != nil || accessToken == "" {
		fmt.Println("获取access token错误")
		return nil, "", err
	}

	// interlaceBaseURL 建议: https://api-sandbox.interlace.money/open-api/v3
	base := interlaceBaseURL + "/cards/transaction-list"

	q := url.Values{}
	q.Set("accountId", in.AccountId)

	if in.ID != "" {
		q.Set("id", in.ID)
	}
	if in.ClientTransactionId != "" {
		q.Set("clientTransactionId", in.ClientTransactionId)
	}
	if in.CardId != "" {
		q.Set("cardId", in.CardId)
	}
	if in.Type != "" {
		q.Set("type", in.Type)
	}
	if in.Status != "" {
		q.Set("status", in.Status)
	}
	if in.StartTime != "" {
		q.Set("startTime", in.StartTime)
	}
	if in.EndTime != "" {
		q.Set("endTime", in.EndTime)
	}

	limit := in.Limit
	if limit <= 0 {
		limit = 10
	}
	page := in.Page
	if page <= 0 {
		page = 1
	}
	q.Set("limit", fmt.Sprintf("%d", limit))
	q.Set("page", fmt.Sprintf("%d", page))

	urlStr := base + "?" + q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, urlStr, nil)
	if err != nil {
		return nil, "", err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("x-access-token", accessToken)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, "", err
	}

	// fmt.Println("txn-list resp:", string(body))

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		fmt.Println("at", interlaceAuth, time.Now().Unix())
		return nil, "", fmt.Errorf("interlace txn list http %d: %s", resp.StatusCode, string(body))
	}

	var outer InterlaceTxnListResp
	if err := json.Unmarshal(body, &outer); err != nil {
		return nil, "", fmt.Errorf("txn list unmarshal: %w", err)
	}
	if outer.Code != "000000" {
		return nil, "", fmt.Errorf("txn list failed: code=%s msg=%s", outer.Code, outer.Message)
	}

	res := make([]*InterlaceTransaction, 0, len(outer.Data.List))
	for i := range outer.Data.List {
		t := outer.Data.List[i]
		res = append(res, &t)
	}

	return res, outer.Data.Total, nil
}

type InterlaceFreezeCardReq struct {
	AccountId string `json:"accountId"`
}

type InterlaceFreezeCardResp struct {
	Code    string        `json:"code"`
	Message string        `json:"message"`
	Data    InterlaceCard `json:"data"` // 你之前那个卡结构体，含 status 等字段
}

// 单张卡片信息（输出）
type InterlaceCard struct {
	ID        string `json:"id"`
	AccountID string `json:"accountId"`
	Status    string `json:"status"`   // INACTIVE, CONTROL, ACTIVE, PENDING, FROZEN
	Currency  string `json:"currency"` // 货币代码
	Bin       string `json:"bin"`

	UserName     string `json:"userName"`
	CreateTime   string `json:"createTime"`
	CardLastFour string `json:"cardLastFour"`

	BillingAddress *InterlaceBillingAddress `json:"billingAddress"`

	Label        string `json:"label"`
	BalanceID    string `json:"balanceId"`
	BudgetID     string `json:"budgetId"`
	CardholderID string `json:"cardholderId"`
	ReferenceID  string `json:"referenceId"`

	CardMode string `json:"cardMode"` // PHYSICAL_CARD / VIRTUAL_CARD

	TransactionLimits []InterlaceTransactionLimit `json:"transactionLimits"`
}

// 账单地址
type InterlaceBillingAddress struct {
	AddressLine1 string `json:"addressLine1,omitempty"`
	AddressLine2 string `json:"addressLine2,omitempty"`
	City         string `json:"city,omitempty"`
	State        string `json:"state,omitempty"`
	PostalCode   string `json:"postalCode,omitempty"`
	Country      string `json:"country,omitempty"`
}

// 单个额度限制
type InterlaceTransactionLimit struct {
	Type     string `json:"type"`     // DAY/WEEK/MONTH/QUARTER/YEAR/LIFETIME/TRANSACTION/NA
	Value    string `json:"value"`    // 金额（字符串）
	Currency string `json:"currency"` // 货币
}

// InterlaceFreezeCard 冻结卡片（返回卡详情，status 应该变成 FROZEN）
func InterlaceFreezeCard(ctx context.Context, accountId, cardId string) (*InterlaceCard, error) {
	if accountId == "" {
		return nil, fmt.Errorf("accountId is required")
	}
	if cardId == "" {
		return nil, fmt.Errorf("cardId is required")
	}

	accessToken, err := GetInterlaceAccessToken(ctx)
	if err != nil || accessToken == "" {
		fmt.Println("获取access token错误")
		return nil, err
	}

	// interlaceBaseURL 建议为: https://api-sandbox.interlace.money/open-api/v3
	urlStr := interlaceBaseURL + "/cards/" + cardId + "/freeze"

	// body: { "accountId": "..." }
	reqBody := &InterlaceFreezeCardReq{AccountId: accountId}
	bs, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("freeze card marshal: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, urlStr, bytes.NewReader(bs))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-access-token", accessToken)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	// fmt.Println("freeze resp:", string(body))

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		fmt.Println("at", interlaceAuth, time.Now().Unix())
		return nil, fmt.Errorf("interlace freeze card http %d: %s", resp.StatusCode, string(body))
	}

	var outer InterlaceFreezeCardResp
	if err := json.Unmarshal(body, &outer); err != nil {
		return nil, fmt.Errorf("freeze card unmarshal: %w", err)
	}
	if outer.Code != "000000" {
		return nil, fmt.Errorf("freeze card failed: code=%s msg=%s", outer.Code, outer.Message)
	}

	return &outer.Data, nil
}

type InterlaceSetCardPinReq struct {
	Pin       string `json:"pin"`       // 6位数字字符串
	AccountId string `json:"accountId"` // 账户UUID
}

type InterlaceSetCardPinResp struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Data    struct {
		Success bool `json:"success"`
	} `json:"data"`
}

// InterlaceSetCardPin 设置卡片 PIN（交易PIN/ATM PIN）
func InterlaceSetCardPin(ctx context.Context, cardId string, in *InterlaceSetCardPinReq) (bool, error) {
	if cardId == "" {
		return false, fmt.Errorf("cardId is required")
	}
	if in == nil {
		return false, fmt.Errorf("set pin req is nil")
	}
	if in.AccountId == "" {
		return false, fmt.Errorf("accountId is required")
	}
	if in.Pin == "" {
		return false, fmt.Errorf("pin is required")
	}
	// 你如果想严格一点，可以只校验长度，不校验数字字符（按你风格）
	if len(in.Pin) != 6 {
		return false, fmt.Errorf("pin length must be 6")
	}

	accessToken, err := GetInterlaceAccessToken(ctx)
	if err != nil || accessToken == "" {
		fmt.Println("获取access token错误")
		return false, err
	}

	// interlaceBaseURL 建议: https://api-sandbox.interlace.money/open-api/v3
	base := interlaceBaseURL + "/cards/" + cardId + "/pin"

	bodyBytes, err := json.Marshal(in)
	if err != nil {
		return false, fmt.Errorf("marshal set pin body: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base, bytes.NewReader(bodyBytes))
	if err != nil {
		return false, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-access-token", accessToken)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return false, err
	}

	fmt.Println("set-pin resp:", string(respBody))

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		fmt.Println("at", interlaceAuth, time.Now().Unix())
		return false, fmt.Errorf("interlace set pin http %d: %s", resp.StatusCode, string(respBody))
	}

	var outer InterlaceSetCardPinResp
	if err := json.Unmarshal(respBody, &outer); err != nil {
		return false, fmt.Errorf("set pin unmarshal: %w", err)
	}
	if outer.Code != "000000" {
		return false, fmt.Errorf("set pin failed: code=%s msg=%s", outer.Code, outer.Message)
	}

	return outer.Data.Success, nil
}
